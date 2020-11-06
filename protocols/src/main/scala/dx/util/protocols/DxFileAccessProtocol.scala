package dx.util.protocols

import java.nio.charset.Charset
import java.nio.file.Path

import dx.api.{DxApi, DxFile, DxFileDescCache}
import dx.util.{AbstractAddressableFileNode, FileAccessProtocol, FileSource, FileUtils}

case class DxFileSource(dxFile: DxFile, override val encoding: Charset)(
    override val address: String,
    dxApi: DxApi
) extends AbstractAddressableFileNode(address, encoding) {

  override def name: String = dxFile.describe().name

  override def folder: String = dxFile.describe().folder

  override lazy val size: Long = dxFile.describe().size

  override def readBytes: Array[Byte] = {
    dxApi.downloadBytes(dxFile)
  }

  override protected def localizeTo(file: Path): Unit = {
    dxApi.downloadFile(file, dxFile)
  }
}

/**
  * Implementation of FileAccessProtocol for dx:// URIs
  * @param dxApi DxApi instance.
  * @param dxFileCache Vector of DxFiles that have already been described (matching is done by file+project IDs)
  * @param encoding character encoding, for resolving binary data.
  */
case class DxFileAccessProtocol(dxApi: DxApi = DxApi.get,
                                dxFileCache: DxFileDescCache = DxFileDescCache.empty,
                                encoding: Charset = FileUtils.DefaultEncoding)
    extends FileAccessProtocol {
  override val schemes = Vector(DxFileAccessProtocol.DxUriScheme)
  private var uriToFileSource: Map[String, DxFileSource] = Map.empty

  private def resolveFileUri(uri: String): DxFile = {
    dxApi.resolveFile(uri.split("::")(0))
  }

  override def resolve(uri: String): DxFileSource = {
    // First search in the fileInfoList. This may save us an API call.
    uriToFileSource.get(uri) match {
      case Some(src) => src
      case None =>
        val dxFile = dxFileCache.updateFileFromCache(resolveFileUri(uri))
        val src = DxFileSource(dxFile, encoding)(uri, dxApi)
        uriToFileSource += (uri -> src)
        src
    }
  }

  def resolveNoCache(uri: String): FileSource = {
    DxFileSource(resolveFileUri(uri), encoding)(uri, dxApi)
  }

  def fromDxFile(dxFile: DxFile): DxFileSource = {
    DxFileSource(dxFile, encoding)(dxFile.asUri, dxApi)
  }
}

object DxFileAccessProtocol {
  val DxUriScheme = "dx"

  def fromDxFile(dxFile: DxFile, protocols: Vector[FileAccessProtocol]): DxFileSource = {
    protocols
      .collectFirst {
        case dx: DxFileAccessProtocol => dx.fromDxFile(dxFile)
      }
      .getOrElse(
          throw new RuntimeException("No dx protocol")
      )
  }
}
