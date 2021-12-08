package dx.util.protocols

import java.nio.charset.Charset
import java.nio.file.Path
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.core.sync.ResponseTransformer
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{
  GetObjectRequest,
  GetObjectResponse,
  ListObjectsV2Request,
  NoSuchBucketException,
  NoSuchKeyException,
  S3Object
}
import dx.util.{
  AbstractAddressableFileNode,
  AddressableFileSource,
  FileAccessProtocol,
  FileSource,
  FileUtils,
  Logger
}

import scala.jdk.CollectionConverters._

/**
  * Represents a file in an S3 bucket.
  * @param address the original s3 uri
  * @param bucketName the bucket
  * @param objectKey the object key
  */
case class S3FileSource(
    override val address: String,
    bucketName: String,
    objectKey: String
)(protocol: S3FileAccessProtocol, private val cachedParent: Option[S3FolderSource] = None)
    extends AbstractAddressableFileNode(address, protocol.encoding) {

  private lazy val request: GetObjectRequest =
    GetObjectRequest.builder().bucket(bucketName).key(objectKey).build()
  private[protocols] lazy val objectPath: PosixPath = PosixPath(objectKey)

  override def name: String = objectPath.name

  override def folder: String = objectPath.getParent.map(_.toString).getOrElse("")

  override def container: String = s"${S3FileAccessProtocol.S3Scheme}:${bucketName}:${folder}"

  // TODO: handle version
  override def version: Option[String] = None

  override def exists: Boolean = {
    try {
      protocol.getClient.getObject(request)
      true
    } catch {
      case _: NoSuchKeyException => false
    }
  }

  override lazy val getParent: Option[S3FolderSource] = {
    cachedParent.orElse {
      if (folder == "") {
        val newUri = s"s3://${bucketName}/"
        Some(S3FolderSource(newUri, bucketName, "/")(protocol))
      } else {
        val newUri = s"s3://${bucketName}/${folder}"
        Some(S3FolderSource(newUri, bucketName, folder)(protocol))
      }
    }
  }

  override def resolve(path: String): S3FileSource = {
    getParent.get.resolve(path)
  }

  override def resolveDirectory(path: String): S3FolderSource = {
    getParent.get.resolveDirectory(path)
  }

  override def relativize(fileSource: AddressableFileSource): String = {
    getParent.get.relativize(fileSource)
  }

  override lazy val size: Long = {
    protocol.getClient.getObject(request).response().contentLength()
  }

  override def readBytes: Array[Byte] = {
    protocol.getClient
      .getObject(request, ResponseTransformer.toBytes[GetObjectResponse]())
      .asByteArray()
  }

  override protected def localizeTo(file: Path): Unit = {
    protocol.getClient.getObject(request, ResponseTransformer.toFile[GetObjectResponse](file))
  }
}

case class S3FolderSource(override val address: String, bucketName: String, prefix: String)(
    protocol: S3FileAccessProtocol,
    private val cachedParent: Option[S3FolderSource] = None,
    private var cachedListing: Option[Vector[FileSource]] = None
) extends AddressableFileSource {
  private lazy val prefixPath: PosixPath = PosixPath(prefix)

  override def name: String = prefixPath.name

  override def folder: String = prefixPath.getParent.map(_.toString).getOrElse("")

  override def container: String = s"${S3FileAccessProtocol.S3Scheme}:${bucketName}:${folder}"

  override val isDirectory: Boolean = true

  private def listPrefix: Vector[S3Object] = {
    // list objects with the given prefix - s3 doesn't have "folders", so we
    // just assume that all objects with the same prefix are in the same folder
    val listRequest = ListObjectsV2Request
      .builder()
      .bucket(bucketName)
      .prefix(prefix)
      .delimiter("/")
      .build()
    val response = protocol.getClient.listObjectsV2(listRequest)
    response.contents().asScala.toVector
  }

  override def exists: Boolean = {
    try {
      listPrefix.nonEmpty
    } catch {
      case _: NoSuchBucketException => false
    }
  }

  override lazy val getParent: Option[S3FolderSource] = {
    cachedParent.orElse {
      if (folder == "") {
        None
      } else {
        val newUri = s"s3://${bucketName}/${folder}"
        Some(S3FolderSource(newUri, bucketName, folder)(protocol))
      }
    }
  }

  override def resolve(path: String): S3FileSource = {
    val objectKey = prefixPath.resolve(path)
    val newUri = s"s3://${bucketName}/${objectKey.toString}"
    // we can only use this folder as the parent if it is the direct ancestor
    // of the new file/folder
    val cachedParent = if (objectKey.getParent == prefixPath) Some(this) else None
    S3FileSource(newUri, bucketName, objectKey.toString)(protocol, cachedParent)
  }

  override def resolveDirectory(path: String): S3FolderSource = {
    val objectKey = prefixPath.resolve(path)
    val newUri = s"s3://${bucketName}/${objectKey.toString}"
    // we can only use this folder as the parent if it is the direct ancestor
    // of the new file/folder
    val cachedParent = if (objectKey.getParent == prefixPath) Some(this) else None
    S3FolderSource(newUri, bucketName, objectKey.toString)(protocol, cachedParent)
  }

  override def relativize(fileSource: AddressableFileSource): String = {
    fileSource match {
      case fs: S3FileSource =>
        prefixPath.relativize(fs.objectPath).toString
      case fs: S3FolderSource =>
        prefixPath.relativize(fs.prefixPath).toString
      case _ =>
        throw new Exception(s"not an S3FileSource: ${fileSource}")
    }
  }

  override protected def localizeTo(dir: Path): Unit = {
    val sourcePath = FileUtils.getPath(prefix)
    listPrefix.foreach { s3obj =>
      val objectKey = s3obj.key()
      val relativePath = sourcePath.relativize(FileUtils.getPath(objectKey))
      val destPath = dir.resolve(relativePath)
      val downloadRequest = GetObjectRequest.builder().bucket(bucketName).key(objectKey).build()
      protocol.getClient.getObject(downloadRequest,
                                   ResponseTransformer.toFile[GetObjectResponse](destPath))
    }
  }

  override def listing(recursive: Boolean = false): Vector[FileSource] = {
    val s3Objs = listPrefix
    if (s3Objs.isEmpty) {
      return Vector.empty
    }

    if (recursive) {
      // add all missing parent dirs
      def addDirs(
          path: Option[Path],
          dirs: Map[Option[Path], (Set[Path], Set[(Path, S3Object)])]
      ): Map[Option[Path], (Set[Path], Set[(Path, S3Object)])] = {
        if (dirs.contains(path)) {
          dirs
        } else {
          val parent = path.flatMap(p => Option(p.getParent))
          val newDirs = if (dirs.contains(parent)) {
            dirs
          } else {
            addDirs(parent, dirs)
          }
          val (subdirs, files) = newDirs(parent)
          newDirs ++ Map(
              parent -> (subdirs + path.get, files),
              path -> (Set.empty[Path], Set.empty[(Path, S3Object)])
          )
        }
      }

      val dirs =
        s3Objs.foldLeft(Map(Option.empty[Path] -> (Set.empty[Path], Set.empty[(Path, S3Object)]))) {
          case (dirs, s3Obj) =>
            val path = FileUtils.getPath(s3Obj.key())
            val parent = Option(path.getParent)
            val newDirs = addDirs(parent, dirs)
            val (subdirs, files) = newDirs(parent)
            newDirs + (parent -> (subdirs, files + ((path, s3Obj))))
        }

      def buildListing(parent: Option[Path], parentSource: S3FolderSource): Vector[FileSource] = {
        val (subdirs, files) = dirs(parent)
        val fileSources = files.toVector.map {
          case (file, s3Obj) =>
            S3FileSource(s"s3://${bucketName}/${file.toString}", bucketName, s3Obj.key())(
                protocol,
                Some(parentSource)
            )
        }
        val folderSources = subdirs.toVector.map { dir =>
          val folder =
            S3FolderSource(s"s3://${bucketName}/${dir.toString}", bucketName, dir.toString)(
                protocol,
                Some(parentSource)
            )
          folder.cachedListing = Some(buildListing(Some(dir), folder))
          folder
        }
        fileSources ++ folderSources
      }

      buildListing(None, this)
    } else {
      val sourcePath = FileUtils.getPath(prefix)
      val (files, folders) =
        s3Objs.foldLeft(Vector.empty[S3FileSource], Map.empty[Path, S3FolderSource]) {
          case ((files, folders), s3Obj) =>
            val relPath = sourcePath.relativize(FileUtils.getPath(s3Obj.key()))
            if (relPath.getNameCount == 1) {
              (files :+ S3FileSource(s"s3://${bucketName}/${s3Obj.key()}", bucketName, s3Obj.key())(
                   protocol,
                   Some(this)
               ),
               folders)
            } else if (relPath.getNameCount == 2 && !folders.contains(relPath.getParent)) {
              val folder = sourcePath.resolve(relPath.getParent).toString
              (files,
               folders + (relPath.getParent -> S3FolderSource(s"s3://${bucketName}/${folder}",
                                                              bucketName,
                                                              folder)(protocol, Some(this))))
            } else {
              (files, folders)
            }
        }
      files ++ folders.values.toVector
    }
  }
}

case class S3FileAccessProtocol(region: Region,
                                credentialsProvider: Option[AwsCredentialsProvider] = None,
                                encoding: Charset = FileUtils.DefaultEncoding)
    extends FileAccessProtocol {
  private var client: Option[S3Client] = None
  private[protocols] def getClient: S3Client = {
    if (client.isEmpty) {
      val builder = S3Client.builder().region(region)
      client = Some(credentialsProvider.map(builder.credentialsProvider).getOrElse(builder).build())
    }
    client.get
  }

  override val schemes: Vector[String] = Vector(S3FileAccessProtocol.S3Scheme)

  override val supportsDirectories: Boolean = true

  // TODO: handle version
  //  how is version specified (I think there is a ?version=<version>)
  private val S3UriRegexp = "s3://(.+?)/(.*)".r

  /**
    * Resolve an S3 URI.
    * Note: this only handles S3 URIs (e.g. "s3://mybucket/test1.txt"), not http(s) URLs.
    *
    * @param uri the S3 URI
    * @return FileSource
    */
  override def resolve(uri: String): S3FileSource = {
    uri match {
      case S3UriRegexp(bucketName, objectKey) if objectKey.nonEmpty && !objectKey.endsWith("/") =>
        S3FileSource(uri, bucketName, objectKey)(this)
      case _ =>
        throw new Exception(s"invalid s3 file URI ${uri}")
    }
  }

  /**
    * Resolve an S3 URI that points to a "folder".
    *
    * @param uri the directory URI
    * @return FileSource
    */
  override def resolveDirectory(uri: String): S3FolderSource = {
    uri match {
      case S3UriRegexp(bucketName, objectKey) if objectKey.isEmpty =>
        S3FolderSource(uri, bucketName, "/")(this)
      case S3UriRegexp(bucketName, objectKey) if objectKey.endsWith("/") =>
        S3FolderSource(uri, bucketName, objectKey)(this)
      case _ =>
        throw new Exception(s"invalid s3 folder URI ${uri}")
    }
  }

  /**
    * Perform any cleanup/shutdown activities. Called immediately before the program exits.
    */
  override def onExit(): Unit = {
    if (client.isDefined) {
      try {
        client.get.close()
      } catch {
        case ex: Throwable =>
          Logger.error("error closing s3 client", Some(ex))
      }
    }
  }
}

object S3FileAccessProtocol {
  val S3Scheme = "s3"

  /**
    * Creates a S3FileAccessProtocol for the AWS region corresponding to the given
    * dx region.
    * @param dxRegion the DNAnexus region
    * @return
    */
  def create(dxRegion: String,
             credentialsProvider: Option[AwsCredentialsProvider] = None): S3FileAccessProtocol = {
    val region = Region.of(dxRegion)
    if (!Region.regions().asScala.toSet.contains(region)) {
      throw new Exception(s"invalid aws region ${dxRegion}")
    }
    S3FileAccessProtocol(region, credentialsProvider)
  }
}
