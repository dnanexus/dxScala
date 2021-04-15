package dx.util.protocols

import java.nio.charset.Charset
import java.nio.file.{Files, Path, Paths}
import dx.api.{DxApi, DxFile, DxFileDescCache, DxFindDataObjects, DxPath, DxProject, Field}
import dx.util.{
  AbstractAddressableFileNode,
  AddressableFileSource,
  FileAccessProtocol,
  FileSource,
  FileUtils,
  SysUtils
}

import java.net.URI

case class DxFileSource(dxFile: DxFile, override val encoding: Charset)(
    override val address: String,
    protocol: DxFileAccessProtocol
) extends AbstractAddressableFileNode(address, encoding) {

  override def name: String = dxFile.getName

  override def folder: String = dxFile.describe().folder

  def project: DxProject = {
    dxFile.project
      .getOrElse(protocol.dxApi.project(dxFile.describe(Set(Field.Project)).project))
  }

  override def getParent: Option[DxFolderSource] = {
    Some(DxFolderSource(project, folder)(protocol))
  }

  override def resolve(path: String): AddressableFileSource = {
    getParent.get.resolve(path)
  }

  override def uri: URI = URI.create(dxFile.asUri)

  override lazy val size: Long = dxFile.describe().size

  override def readBytes: Array[Byte] = {
    protocol.dxApi.downloadBytes(dxFile)
  }

  override protected def localizeTo(file: Path): Unit = {
    protocol.dxApi.downloadFile(file, dxFile, overwrite = true)
  }
}

case class DxArchiveFolderSource(dxFileSource: DxFileSource) extends AddressableFileSource {
  override def isDirectory: Boolean = true

  override def address: String = dxFileSource.address

  override def name: String = {
    FileUtils.changeFirstFileExt(dxFileSource.name, Vector(".tar.gz", ".tgz", ".tar"))
  }

  override def folder: String = dxFileSource.folder

  override def getParent: Option[AddressableFileSource] = dxFileSource.getParent

  override def resolve(path: String): AddressableFileSource = dxFileSource.resolve(path)

  override def uri: URI = dxFileSource.uri

  override protected def localizeTo(dir: Path): Unit = {
    val tempfile = Files.createTempFile("temp", dxFileSource.name)
    val tarOpts = if (dxFileSource.name.endsWith("gz")) "-xz" else "-x"
    try {
      dxFileSource.localize(tempfile)
      SysUtils.execCommand(s"tar ${tarOpts} -f ${tempfile} -C ${dir.toString} --strip-components=1")
    } finally {
      tempfile.toFile.delete()
    }
  }
}

case class DxFolderSource(dxProject: DxProject, folder: String)(
    protocol: DxFileAccessProtocol
) extends AddressableFileSource {
  override def address: String = s"dx://${dxProject.id}:${folder}"

  override def name: String = folder

  override def isDirectory: Boolean = true

  override def getParent: Option[DxFolderSource] = {
    if (folder == "/") {
      None
    } else {
      val parent = Paths.get(folder).getParent.toString match {
        case p if p.endsWith("/") => p
        case p                    => s"${p}/"
      }
      Some(DxFolderSource(dxProject, parent)(protocol))
    }
  }

  override def resolve(path: String): AddressableFileSource = {
    if (path.endsWith("/")) {
      DxFolderSource(dxProject, s"${folder}${path}")(protocol)
    } else {
      val uri = s"dx://${dxProject.id}:${folder}${path}"
      protocol.resolve(uri)
    }
  }

  lazy val listing: Vector[(DxFile, Path)] = {
    val results =
      DxFindDataObjects(protocol.dxApi)
        .apply(Some(dxProject), Some(folder), recurse = true, Some("file"))
    results.map {
      case (f: DxFile, _) =>
        val relPath = Paths.get(folder).relativize(Paths.get(f.describe().folder))
        (f, relPath)
      case other => throw new Exception(s"unexpected result ${other}")
    }.toVector
  }

  override protected def localizeTo(dir: Path): Unit = {
    listing.foreach {
      case (dxFile, relPath) =>
        val path = dir.resolve(relPath).resolve(dxFile.getName)
        protocol.dxApi.downloadFile(path, dxFile, overwrite = true)
    }
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

  override val supportsDirectories: Boolean = true

  private def resolveFileUri(uri: String): DxFile = {
    uri.split("::").toVector match {
      case Vector(uri, fileName) =>
        val dxFile = dxApi.resolveFile(uri)
        if (!dxFile.hasCachedDesc) {
          dxFile.copy()(dxApi = dxApi, name = Some(fileName))
        } else if (dxFile.describe().name != fileName) {
          throw new Exception(
              s"""file ${dxFile} name from file.describe() ${dxFile.describe().name} 
                 |does not match name from URI ${fileName}""".stripMargin.replaceAll("\n", " ")
          )
        } else {
          dxFile
        }
      case Vector(uri) =>
        dxApi.resolveFile(uri)
      case _ =>
        throw new Exception(s"invalid file URI ${uri}")
    }
  }

  private def resolveFile(uri: String): DxFileSource = {
    val dxFile = dxFileCache.updateFileFromCache(resolveFileUri(uri))
    val src = DxFileSource(dxFile, encoding)(uri, this)
    uriToFileSource += (uri -> src)
    src
  }

  override def resolve(uri: String): DxFileSource = {
    // First search in the cache. This may save us an API call.
    uriToFileSource.get(uri) match {
      case Some(src) => src
      case None      => resolveFile(uri)
    }
  }

  override def resolveDirectory(uri: String): AddressableFileSource = {
    // a Directory may be either a dx file or a dx://project:/path/to/dir/ URI.
    if (uri.endsWith("/")) {
      val (projectName, folder) = DxPath.split(uri)
      val project = projectName
        .map(dxApi.resolveProject)
        .getOrElse(throw new Exception("project must be specified for a DNAnexus folder URI"))
      DxFolderSource(project, folder)(this)
    } else {
      DxArchiveFolderSource(resolveFile(uri))
    }
  }

  def resolveNoCache(uri: String): FileSource = {
    DxFileSource(resolveFileUri(uri), encoding)(uri, this)
  }

  def fromDxFile(dxFile: DxFile): DxFileSource = {
    DxFileSource(dxFile, encoding)(dxFile.asUri, this)
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
