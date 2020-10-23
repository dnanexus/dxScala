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
  ListObjectsV2Request
}
import dx.util.{
  AbstractAddressableFileNode,
  AddressableFileSource,
  FileAccessProtocol,
  FileUtils,
  Logger
}

import scala.jdk.CollectionConverters._

/**
  * Represents a file in an S3 bucket.
  * @param address the original s3 uri
  * @param client the S3Client
  * @param bucketName the bucket
  * @param objectKey the object key
  * @param encoding character encoding
  */
case class S3FileSource(override val address: String,
                        client: S3Client,
                        bucketName: String,
                        objectKey: String,
                        override val isDirectory: Boolean,
                        override val encoding: Charset)
    extends AbstractAddressableFileNode(address, encoding) {

  private lazy val request: GetObjectRequest =
    GetObjectRequest.builder().bucket(bucketName).key(objectKey).build()
  private lazy val objectPath: Path = Paths.get(objectKey)

  override def name: String = objectPath.getFileName.toString

  override def folder: String = objectPath.getParent match {
    case null => ""
    case path => path.toString
  }

  override lazy val size: Long = {
    client.getObject(request).response().contentLength()
  }

  override def readBytes: Array[Byte] = {
    client.getObject(request, ResponseTransformer.toBytes[GetObjectResponse]()).asByteArray()
  }

  override protected def localizeTo(file: Path): Unit = {
    client.getObject(request, ResponseTransformer.toFile[GetObjectResponse](file))
  }
}

case class S3FolderSource(override val address: String,
                          client: S3Client,
                          bucketName: String,
                          prefix: String)
    extends AddressableFileSource {
  private lazy val prefixPath: Path = Paths.get(prefix)

  override def name: String = prefixPath.getFileName.toString

  override def folder: String = prefixPath.getParent match {
    case null => ""
    case path => path.toString
  }

  override val isDirectory: Boolean = true

  override protected def localizeTo(dir: Path): Unit = {
    // list objects with the given prefix - s3 doesn't have "folders", so we
    // just assume that all objects with the same prefix are in the same folder
    val listRequest = ListObjectsV2Request
      .builder()
      .bucket(bucketName)
      .prefix(prefix)
      .delimiter("/")
      .build()
    val response = client.listObjectsV2(listRequest)
    val sourcePath = Paths.get(prefix)
    response.contents().asScala.foreach { s3obj =>
      val objectKey = s3obj.key()
      val relativePath = sourcePath.relativize(Paths.get(objectKey))
      val destPath = dir.resolve(relativePath)
      val downloadRequest = GetObjectRequest.builder().bucket(bucketName).key(objectKey).build()
      client.getObject(downloadRequest, ResponseTransformer.toFile[GetObjectResponse](destPath))
    }
  }
}

case class S3FileAccessProtocol(region: Region,
                                credentialsProvider: Option[AwsCredentialsProvider] = None,
                                encoding: Charset = FileUtils.DefaultEncoding)
    extends FileAccessProtocol {
  private var client: Option[S3Client] = None
  private def getClient: S3Client = {
    if (client.isEmpty) {
      val builder = S3Client.builder().region(region)
      client = Some(credentialsProvider.map(builder.credentialsProvider).getOrElse(builder).build())
    }
    client.get
  }

  override val schemes: Vector[String] = Vector("s3")

  override val supportsDirectories: Boolean = true

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
        S3FileSource(uri, getClient, bucketName, objectKey, isDirectory = false, encoding)
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
        S3FolderSource(uri, getClient, bucketName, "/")
      case S3UriRegexp(bucketName, objectKey) if objectKey.endsWith("/") =>
        S3FolderSource(uri, getClient, bucketName, objectKey)
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
