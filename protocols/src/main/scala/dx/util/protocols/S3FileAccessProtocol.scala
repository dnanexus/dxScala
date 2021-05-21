package dx.util.protocols

import java.nio.charset.Charset
import java.nio.file.{Path, Paths}
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
)(protocol: S3FileAccessProtocol)
    extends AbstractAddressableFileNode(address, protocol.encoding) {

  private lazy val request: GetObjectRequest =
    GetObjectRequest.builder().bucket(bucketName).key(objectKey).build()
  private[protocols] lazy val objectPath: Path = Paths.get(objectKey)

  override def name: String = objectPath.getFileName.toString

  override def folder: String = objectPath.getParent match {
    case null => ""
    case path => path.toString
  }

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

  override def getParent: Option[S3FolderSource] = {
    if (folder == "") {
      val newUri = s"s3://${bucketName}/"
      Some(S3FolderSource(newUri, bucketName, "/")(protocol))
    } else {
      val newUri = s"s3://${bucketName}/${folder}"
      Some(S3FolderSource(newUri, bucketName, folder)(protocol))
    }
  }

  override def resolve(path: String): AddressableFileSource = {
    getParent.get.resolve(path)
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
    protocol: S3FileAccessProtocol
) extends AddressableFileSource {
  private lazy val prefixPath: Path = Paths.get(prefix)

  override def name: String = prefixPath.getFileName.toString

  override def folder: String = prefixPath.getParent match {
    case null => ""
    case path => path.toString
  }

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

  override def getParent: Option[S3FolderSource] = {
    if (folder == "") {
      None
    } else {
      val newUri = s"s3://${bucketName}/${folder}"
      Some(S3FolderSource(newUri, bucketName, folder)(protocol))
    }
  }

  override def resolve(path: String): AddressableFileSource = {
    val objectKey = s"${folder}${path}"
    val newUri = s"s3://${bucketName}/${objectKey}"
    if (path.endsWith("/")) {
      S3FolderSource(newUri, bucketName, objectKey)(protocol)
    } else {
      S3FileSource(newUri, bucketName, objectKey)(protocol)
    }
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
    val sourcePath = Paths.get(prefix)
    listPrefix.foreach { s3obj =>
      val objectKey = s3obj.key()
      val relativePath = sourcePath.relativize(Paths.get(objectKey))
      val destPath = dir.resolve(relativePath)
      val downloadRequest = GetObjectRequest.builder().bucket(bucketName).key(objectKey).build()
      protocol.getClient.getObject(downloadRequest,
                                   ResponseTransformer.toFile[GetObjectResponse](destPath))
    }
  }

  override def listing: Vector[FileSource] = {
    val sourcePath = Paths.get(prefix)
    val relPaths = listPrefix.map(s3obj => sourcePath.relativize(Paths.get(s3obj.key())) -> s3obj)
    val files = relPaths.collect {
      case (relPath, s3Obj) if relPath.getNameCount == 1 =>
        S3FileSource(s"s3://${bucketName}/${s3Obj.key()}", bucketName, s3Obj.key())(protocol)
    }
    val folders = relPaths
      .collect {
        case (relPath, _) if relPath.getNameCount == 2 =>
          sourcePath.resolve(relPath.getParent).toString
      }
      .toSet
      .map { folder: String =>
        S3FolderSource(s"s3://${bucketName}/${folder}", bucketName, folder)(protocol)
      }
    files ++ folders.toVector
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
