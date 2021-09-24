package dx.util.protocols

import com.dnanexus.exceptions.ResourceNotFoundException

import java.nio.charset.Charset
import java.nio.file.{Files, Path, Paths}
import dx.api.{DxApi, DxFile, DxFileDescCache, DxFindDataObjects, DxPath, DxProject, DxState, Field}
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
    protocol: DxFileAccessProtocol,
    private val cachedParent: Option[DxFolderSource] = None
) extends AbstractAddressableFileNode(address, encoding) {

  override def name: String = dxFile.getName

  override def folder: String = dxFile.describe().folder

  def dxProject: DxProject = {
    dxFile.project
      .getOrElse(protocol.dxApi.project(dxFile.describe(Set(Field.Project)).project))
  }

  override def container: String = s"${DxPath.DxScheme}:${dxProject.id}:${folder}"

  override def version: Option[String] = Some(dxFile.id)

  override def exists: Boolean = {
    dxFile.describeNoCache(Set(Field.State)).state == DxState.Closed
  }

  override def getParent: Option[DxFolderSource] = {
    cachedParent.orElse(
        Some(DxFolderSource(dxProject, DxFolderSource.ensureEndsWithSlash(folder))(protocol))
    )
  }

  override def resolve(path: String): DxFileSource = {
    getParent.get.resolve(path)
  }

  override def resolveDirectory(path: String): DxFolderSource = {
    getParent.get.resolveDirectory(path)
  }

  override def relativize(fileSource: AddressableFileSource): String = {
    getParent.get.relativize(fileSource)
  }

  override def uri: URI = URI.create(dxFile.asUri)

  override lazy val size: Long = dxFile.describe().size

  override def readBytes: Array[Byte] = {
    protocol.dxApi.downloadBytes(dxFile)
  }

  override protected def localizeTo(file: Path): Unit = {
    protocol.dxApi.downloadFile(file, dxFile, overwrite = true)
  }

  // copies this file and sets the given parent as this file's cachedParent
  // only if this file's parent folder matches the parent's folder
  private[protocols] def copyWithParent(parent: DxFolderSource): DxFileSource = {
    if (FileUtils.getPath(folder) == parent.dxFolderPath) {
      copy()(address, protocol, Some(parent))
    } else {
      this
    }
  }
}

object DxFileSource {
  def isDxFileUri(uri: String): Boolean = {
    uri.startsWith(DxPath.DxUriPrefix) && uri.contains("file-") && !uri.endsWith("/")
  }
}

case class DxArchiveFolderSource(dxFileSource: DxFileSource) extends AddressableFileSource {
  override def isDirectory: Boolean = true

  override def address: String = dxFileSource.address

  override def name: String = {
    FileUtils.changeFirstFileExt(dxFileSource.name, Vector(".tar.gz", ".tgz", ".tar"))
  }

  override def folder: String = dxFileSource.folder

  override def container: String = dxFileSource.container

  override def version: Option[String] = dxFileSource.version

  override def exists: Boolean = {
    dxFileSource.exists
  }

  override def getParent: Option[AddressableFileSource] = dxFileSource.getParent

  override def resolve(path: String): AddressableFileSource = dxFileSource.resolve(path)

  override def resolveDirectory(path: String): AddressableFileSource =
    dxFileSource.resolveDirectory(path)

  override def relativize(fileSource: AddressableFileSource): String =
    dxFileSource.relativize(fileSource)

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

  private[protocols] def copyWithParent(parent: DxFolderSource): DxArchiveFolderSource = {
    copy(dxFileSource.copyWithParent(parent))
  }
}

/**
  * Represents a folder in a DNAnexus project.
  * @param dxProject the DNAnexus project (`DxProject`) object
  * @param dxFolder the absolute target folder - must terminate with
  *               a '/' (e.g. /a/b/c/)
  * @param parentProjectFolder if this is a folder in a temporary container, the folder in the parent project where this
  *                            folder will  be cloned at the end of the successful job/analysis. Note that this is just
  *                            for bookkeeping - it is not used internally.
  * @param protocol DxFileAccessProtocol
  */
case class DxFolderSource(dxProject: DxProject,
                          dxFolder: String,
                          parentProjectFolder: Option[String] = None)(
    protocol: DxFileAccessProtocol,
    private val cachedParent: Option[DxFolderSource] = None,
    private var cachedListing: Option[Vector[FileSource]] = None
) extends AddressableFileSource {
  private[protocols] val dxFolderPath = FileUtils.getPath(dxFolder)
  assert(dxFolderPath.isAbsolute, s"not an absolute path: ${dxFolderPath}")
  assert(dxFolder.endsWith("/"), "dx folder must end with '/'")

  override def address: String = DxFolderSource.format(dxProject, dxFolder, parentProjectFolder)

  override def name: String = dxFolderPath.getFileName.toString

  override def folder: String = Option(dxFolderPath.getParent).map(_.toString).getOrElse("")

  override def container: String = s"${DxPath.DxScheme}:${dxProject.id}:${folder}"

  override def isDirectory: Boolean = true

  override def exists: Boolean = {
    try {
      dxProject.listFolder(dxFolder)
      true
    } catch {
      case _: ResourceNotFoundException => false
    }
  }

  override def getParent: Option[DxFolderSource] = {
    cachedParent.orElse {
      if (folder == "") {
        None
      } else {
        Some(
            DxFolderSource(dxProject,
                           DxFolderSource.ensureEndsWithSlash(folder),
                           parentProjectFolder)(
                protocol
            )
        )
      }
    }
  }

  // copies this folder and sets the given parent as this folder's cachedParent
  // only if this folder's parent folder matches the parent's folder
  private[protocols] def copyWithParent(parent: DxFolderSource): DxFolderSource = {
    if (FileUtils.getPath(folder) == parent.dxFolderPath) {
      copy()(protocol, Some(this), cachedListing)
    } else {
      this
    }
  }

  override def resolve(path: String): DxFileSource = {
    val newPath = FileUtils.normalizePath(dxFolderPath.resolve(path))
    val file = protocol.resolve(DxFile.format(dxProject.id, newPath.toString))
    file.copyWithParent(this)
  }

  override def resolveDirectory(path: String): DxFolderSource = {
    val newPath = FileUtils.normalizePath(dxFolderPath.resolve(path))
    val folder =
      DxFolderSource(dxProject,
                     DxFolderSource.ensureEndsWithSlash(newPath.toString),
                     parentProjectFolder)(protocol)
    folder.copyWithParent(this)
  }

  override def relativize(fileSource: AddressableFileSource): String = {
    fileSource match {
      case fs: DxFileSource =>
        dxFolderPath.relativize(Paths.get(fs.folder)).resolve(fs.name).toString
      case fs: DxArchiveFolderSource =>
        dxFolderPath.relativize(Paths.get(fs.folder)).resolve(fs.name).toString
      case fs: DxFolderSource =>
        dxFolderPath.relativize(fs.dxFolderPath).toString
      case _ =>
        throw new Exception(s"not a DxFileSource: ${fileSource}")
    }
  }

  private def listDxFolderRecursive: Vector[DxFile] = {
    DxFindDataObjects(protocol.dxApi)
      .apply(Some(dxProject), Some(dxFolder), recurse = true, Some("file"))
      .keys
      .toVector
      .map {
        case dxFile: DxFile => dxFile
        case other          => throw new Exception(s"unexpected result ${other}")
      }
  }

  override protected def localizeTo(dir: Path): Unit = {
    listDxFolderRecursive.foreach { dxFile =>
      val relPath = dxFolderPath.relativize(Paths.get(dxFile.describe().folder))
      val path = dir.resolve(relPath).resolve(dxFile.getName)
      protocol.dxApi.downloadFile(path, dxFile, overwrite = true)
    }
  }

  override def listing(recursive: Boolean = false): Vector[FileSource] = {
    cachedListing.getOrElse {
      if (recursive) {
        def addDirs(
            folder: Path,
            dirs: Map[Path, (Set[Path], Set[DxFile])]
        ): Map[Path, (Set[Path], Set[DxFile])] = {
          if (dirs.contains(folder)) {
            dirs
          } else if (folder.getParent == null) {
            throw new Exception(s"cannot add folder ${folder} to directory listing")
          } else {
            val newDirs = addDirs(folder.getParent, dirs)
            val (subdirs, files) = newDirs(folder.getParent)
            newDirs ++ Map(
                folder.getParent -> (subdirs + folder, files),
                folder -> (Set.empty[Path], Set.empty[DxFile])
            )
          }
        }

        // use findDataObjects to get all files in a folder recursively with a
        // single API call
        val dirs = listDxFolderRecursive
          .groupBy(dxFile => FileUtils.getPath(dxFile.describe().folder))
          .foldLeft(Map(dxFolderPath -> (Set.empty[Path], Set.empty[DxFile]))) {
            case (dirs, (folder, children)) =>
              val newDirs = addDirs(folder, dirs)
              val (subdirs, files) = newDirs(folder)
              newDirs + (folder -> (subdirs, files ++ children))
          }

        def buildListing(folder: Path, parent: DxFolderSource): Vector[FileSource] = {
          val (subdirs, files) = dirs(folder)
          val fileSources = files.toVector.map { dxFile =>
            DxFileSource(dxFile, protocol.encoding)(dxFile.asUri, protocol, Some(parent))
          }
          val folderSources = subdirs.toVector.map { folder =>
            val fs = DxFolderSource(dxProject,
                                    DxFolderSource.ensureEndsWithSlash(folder.toString),
                                    parentProjectFolder)(
                protocol,
                Some(parent)
            )
            fs.cachedListing = Some(buildListing(folder, fs))
            fs
          }
          fileSources ++ folderSources
        }

        buildListing(dxFolderPath, this)
      } else {
        // using findDataObjects with recurse=false will not give us the subfolders,
        // so we need to use project/listFolder instead
        val contents = dxProject.listFolder(dxFolder)
        val fileSources = contents.dataObjects.collect {
          case dxFile: DxFile =>
            DxFileSource(dxFile, protocol.encoding)(dxFile.asUri, protocol, Some(this))
        }
        val folderSources = contents.subFolders.map { folder =>
          DxFolderSource(dxProject,
                         DxFolderSource.ensureEndsWithSlash(folder),
                         parentProjectFolder)(
              protocol,
              Some(this)
          )
        }
        fileSources ++ folderSources
      }
    }
  }
}

object DxFolderSource {
  def ensureEndsWithSlash(folder: String): String = {
    if (folder.endsWith("/")) {
      folder
    } else {
      s"${folder}/"
    }
  }

  def canonicalizeFolder(folder: String): String = {
    ensureEndsWithSlash(FileUtils.getPath(folder).toString)
  }

  def isDxFolderUri(uri: String): Boolean = {
    uri.startsWith(DxPath.DxUriPrefix) &&
    uri.endsWith("/") &&
    (uri.contains("project-") || uri.contains("container-"))
  }

  def join(folder: String, name: String, isFolder: Boolean = false): String = {
    val joined = s"${ensureEndsWithSlash(folder)}${name}"
    if (isFolder) {
      ensureEndsWithSlash(joined)
    } else {
      joined
    }
  }

  def format(project: DxProject,
             folder: String,
             parentProjectFolder: Option[String] = None): String = {
    val base = s"${DxPath.DxUriPrefix}${project.id}:${ensureEndsWithSlash(folder)}"
    parentProjectFolder.map(f => s"${base}::${f}").getOrElse(base)
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
  override val schemes = Vector(DxPath.DxScheme)
  private var uriToFileSource: Map[String, DxFileSource] = Map.empty

  override val supportsDirectories: Boolean = true

  private def resolveFileUri(uri: String): DxFile = {
    uri.split("::").toVector match {
      case Vector(uri, path) =>
        val dxFile = dxApi.resolveFile(uri)
        val (name, folder) = FileUtils.getPath(path) match {
          case p if p.getNameCount == 1 && !p.isAbsolute =>
            (p.getFileName.toString, None)
          case p =>
            (p.getFileName.toString, Some(p.getParent.toString))
        }
        if (!dxFile.hasCachedDesc) {
          dxFile.copy()(dxApi = dxApi, name = Some(name), folder = folder)
        } else if (dxFile.describe().name != name) {
          throw new Exception(
              s"""file ${dxFile} name from file.describe() ${dxFile.describe().name} 
                 |does not match name from URI ${name}""".stripMargin.replaceAll("\n", " ")
          )
        } else {
          dxFile
        }
      case Vector(uri) => dxApi.resolveFile(uri)
      case _           => throw new Exception(s"invalid file URI ${uri}")
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
    // a Directory may be either a dx file (archive) or a dx://project:/path/to/dir/ URI.
    if (uri.endsWith("/")) {
      val (baseUri, parentProjectPath) = uri.split("::").toVector match {
        case Vector(base, parentProjectPath) => (base, Some(parentProjectPath))
        case Vector(_)                       => (uri, None)
      }
      val (projectName, folder) = DxPath.split(baseUri)
      val project = projectName
        .map(dxApi.resolveProject)
        .getOrElse(throw new Exception("project must be specified for a DNAnexus folder URI"))
      DxFolderSource(project, DxFolderSource.canonicalizeFolder(folder), parentProjectPath)(this)
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

  def fromDxFolder(projectId: String,
                   folder: String,
                   parentProjectFolder: Option[String] = None): DxFolderSource = {
    DxFolderSource(dxApi.project(projectId),
                   DxFolderSource.ensureEndsWithSlash(FileUtils.getPath(folder).toString),
                   parentProjectFolder)(this)
  }
}

object DxFileAccessProtocol {
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
