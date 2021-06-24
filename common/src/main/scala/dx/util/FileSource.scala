package dx.util

import java.io.{ByteArrayOutputStream, FileNotFoundException, FileOutputStream, OutputStream}
import java.net.{HttpURLConnection, URI}
import java.nio.charset.Charset
import java.nio.file.{
  FileAlreadyExistsException,
  FileVisitOption,
  FileVisitResult,
  Files,
  Path,
  Paths,
  SimpleFileVisitor
}
import java.nio.file.attribute.BasicFileAttributes
import java.{util => javautil}

import dx.util.FileUtils.{FileScheme, getUriScheme}

import scala.io.Source
import scala.reflect.ClassTag

/**
  * A FileSource is just that - a source of files. It may represent a single file or
  * a directory of files (such as a local directory or an archive from which files
  * can be extracted). It may be be physically located on local disk, remotely, or
  * in memory.
  */
trait FileSource {

  /**
    * The name of this FileSource.
    */
  def name: String

  /**
    * Whether this FileSource represents a directory.
    */
  def isDirectory: Boolean

  /**
    * Whether the file described by this FileSource exists.
    */
  def exists: Boolean

  /**
    * Localizes this FileSource to the given Path. Implementations assume
    * that if the file exists it may be overwritten.
    */
  protected def localizeTo(file: Path): Unit

  /**
    * Localizes this FileSource to the specified path. The current contents
    * of the file/directory are localized; there is no guarantee that the
    * contents will be the same later (i.e. there is no requirement to keep
    * the local and source copies in sync).
    * @param path destination path
    * @param overwrite whether to overwrite any existing file/directory
    * @return the absolute destination path
    */
  def localize(path: Path, overwrite: Boolean = false): Path = {
    if (Files.exists(path) && !overwrite) {
      throw new FileAlreadyExistsException(
          s"file ${path} already exists and overwrite = false"
      )
    }
    val absFile = FileUtils.absolutePath(path)
    localizeTo(absFile)
    absFile
  }

  /**
    * Localizes this FileSource to the specified parent directory. The current
    * contents of the file/directory are localized; there is no guarantee that
    * the contents will be the same later (i.e. there is no requirement to keep
    * the local and source copies in sync).
    * @param dir the destination parent directory
    * @param overwrite whether to overwrite any existing file/directory
    * @return the absolute destination path
    */
  def localizeToDir(dir: Path, overwrite: Boolean = false): Path = {
    localize(dir.resolve(name), overwrite)
  }

  /**
    * Whether `listing` may be called.
    */
  def isListable: Boolean = isDirectory

  /**
    * If `isDirectory=true` and `isListable=true`, returns a Vector of
    * all the files/directories in this directory; otherwise throws
    * UnsupportedOperationException. The listing reflects the current
    * contents of the directory. If `recursive=true`, the recursive listing
    * of this directory is cached and used to construct the listings of any
    * nested directories.
    */
  def listing(recursive: Boolean = false): Vector[FileSource] = {
    throw new UnsupportedOperationException
  }
}

/**
  * A FileSource that has an address, such as a local file path or a URI.
  */
trait AddressableFileSource extends FileSource {

  /**
    * The original value that was resolved to get this FileSource.
    */
  def address: String

  /**
    * The parent of this FileSource. The value will be `""` in
    * the case that `isDirectory` is `true` and this
    * AddressableFileSource represents a root folder.
    */
  def folder: String

  /**
    * Returns an AddressableFileSource that represents this source's
    * parent directory, or None if this is already the root directory.
    */
  def getParent: Option[AddressableFileSource]

  /**
    * A unique identifier for the location where this file
    * is contained that differentiates two folders with the
    * same name.
    */
  def container: String

  /**
    * A file version, in the case of versioning file systems. Versions
    * must be in lexicographic order, such that earlier versions come
    * before later versions when a Vector of versions is naturally sorted.
    */
  def version: Option[String] = None

  /**
    * Resolves a path relative to this AddressableFileSource if it
    * is a directory, or to it's parent if it's a file.
    * @param path relative path - must end with '/' if it is a directory
    * @return
    */
  def resolve(path: String): AddressableFileSource

  /**
    * Returns the path of `fileSource` relative to this one, if this is a
    * directory, or to the parent, if this is a file. Throws an exception
    * if this is not an ancestor of `fileSource`.
    * @param fileSource the child AddressableFileSource
    * @return
    */
  def relativize(fileSource: AddressableFileSource): String

  def uri: URI = URI.create(address)

  override def toString: String = address
}

/**
  * A FileNode is a FileSource that represents a single phyiscal file.
  * It has a size, and its contents may be read as bytes or a string.
  * A FileNode may be a "directory" - such as an archive file that,
  * when localized, is extracted to a hierarchy of files.
  */
trait FileNode extends FileSource {
  override def isDirectory: Boolean = false

  /**
    * Reads the entire file into a byte array.
    * @return
    */
  def readBytes: Array[Byte]

  /**
    * Reads the entire file into a string.
    * @return
    */
  def readString: String

  /**
    * Reads the entire file into a vector of lines.
    * @return
    */
  def readLines: Vector[String]

  /**
    * The size of the file in bytes.
    */
  def size: Long = readBytes.length

  protected def checkFileSize(): Unit = {
    // check that file isn't too big
    val fileSizeMiB = BigDecimal(size) / FileNode.MiB
    if (fileSizeMiB > FileNode.MaxFileSizeMiB) {
      throw new Exception(
          s"""${toString} size is ${fileSizeMiB} MiB;
             |reading files larger than ${FileNode.MaxFileSizeMiB} MiB is unsupported""".stripMargin
      )
    }
  }
}

trait AddressableFileNode extends FileNode with AddressableFileSource

object FileNode {
  val MiB = BigDecimal(1024 * 1024)
  val MaxFileSizeMiB = BigDecimal(256)
}

abstract class AbstractAddressableFileNode(override val address: String, val encoding: Charset)
    extends AddressableFileNode {

  override def readString: String = {
    new String(readBytes, encoding)
  }

  override def readLines: Vector[String] = {
    Source.fromBytes(readBytes, encoding.name).getLines().toVector
  }
}

case class NoSuchProtocolException(name: String)
    extends Exception(s"Protocol ${name} not supported")

case class ProtocolFeatureNotSupportedException(name: String, feature: String)
    extends Exception(s"Protocol ${name} does not support feature ${feature}")

/**
  * A protocol for resolving FileSources.
  */
trait FileAccessProtocol {

  /**
    * URI schemes that this protocol is able to resolve.
    */
  def schemes: Vector[String]

  /**
    * Whether this protocol supports resolving directories.
    */
  def supportsDirectories = false

  /**
    * Resolves a URI to a FileNode.
    * @param address the file URI
    * @return FileNode
    */
  def resolve(address: String): AddressableFileNode

  /**
    * Resolves a URI that points to a directory. Must only be implemented if `supportsDirectories` is true.
    * @param address the directory URI
    * @return FileSource
    */
  def resolveDirectory(address: String): AddressableFileSource = {
    throw new UnsupportedOperationException
  }

  /**
    * Perform any cleanup/shutdown activities. Called immediately before the program exits.
    */
  def onExit(): Unit = {}
}

/**
  * A FileSource for a local file.
  * @param address the original path/URI used to resolve this file.
  * @param originalPath the original, non-cannonicalized Path determined from `address` - may be relative
  * @param canonicalPath the absolute, cannonical path to this file
  * @param logger the logger
  * @param encoding the file encoding
  * @param isDirectory whether this FileSource represents a directory
  */
case class LocalFileSource(
    canonicalPath: Path,
    override val encoding: Charset,
    override val isDirectory: Boolean
)(override val address: String,
  val originalPath: Path,
  private val cachedParent: Option[LocalFileSource] = None,
  private var cachedListing: Option[Vector[FileSource]] = None,
  logger: Logger)
    extends AbstractAddressableFileNode(address, encoding) {

  override lazy val name: String = canonicalPath.getFileName.toString

  override lazy val folder: String = canonicalPath.getParent match {
    case null   => ""
    case parent => parent.toString
  }

  override def container: String = s"file:${canonicalPath.getRoot.toString}:${folder}"

  // TODO: handle versioning file systems
  override def version: Option[String] = None

  override lazy val uri: URI = canonicalPath.toUri

  override def exists: Boolean = {
    Files.exists(canonicalPath)
  }

  override lazy val getParent: Option[LocalFileSource] = {
    cachedParent.orElse(Option(canonicalPath.getParent).map { parent =>
      LocalFileSource(parent, encoding, isDirectory = true)(parent.toString,
                                                            parent,
                                                            logger = logger)

    })
  }

  override def resolve(path: String): LocalFileSource = {
    val parent = if (isDirectory) this else getParent.get
    val parentPath = parent.canonicalPath
    val newPath = parentPath.resolve(path)
    val newCanonicalPath = FileUtils.normalizePath(newPath)
    cachedListing
      .flatMap { listing =>
        listing.collectFirst {
          case fs: LocalFileSource if fs.canonicalPath == newCanonicalPath => fs
        }
      }
      .getOrElse {
        val newIsDirectory = newCanonicalPath.toFile match {
          case f if f.exists()         => f.isDirectory
          case _ if path.endsWith("/") => true
          case _                       => false
        }
        // the parent can only be used if it is the direct ancestor of the new path
        val cachedParent = if (newCanonicalPath.getParent == parentPath) Some(parent) else None
        LocalFileSource(newCanonicalPath, encoding, newIsDirectory)(newPath.toString,
                                                                    newPath,
                                                                    cachedParent = cachedParent,
                                                                    logger = logger)
      }
  }

  override def relativize(fileSource: AddressableFileSource): String = {
    fileSource match {
      case fs: LocalFileSource if isDirectory =>
        canonicalPath.relativize(fs.canonicalPath).toString
      case fs: LocalFileSource =>
        canonicalPath.getParent.relativize(fs.canonicalPath).toString
      case _ =>
        throw new Exception(s"not a LocalFileSource: ${fileSource}")
    }
  }

  /**
    * Check that `exists` equals the given expected value, otherwise
    * throws an exception.
    */
  def checkExists(expected: Boolean): Unit = {
    val existing = exists
    if (expected && !existing) {
      throw new FileNotFoundException(s"Path does not exist ${canonicalPath}")
    }
    if (!expected && existing) {
      throw new FileAlreadyExistsException(s"Path already exists ${canonicalPath}")
    }
  }

  override lazy val size: Long = {
    checkExists(true)
    if (isDirectory && Files.isDirectory(canonicalPath)) {
      throw new Exception("Cannot get the size of a directory")
    }
    try {
      canonicalPath.toFile.length()
    } catch {
      case t: Throwable =>
        throw new Exception(s"Error getting size of file ${canonicalPath}: ${t.getMessage}")
    }
  }

  override def readBytes: Array[Byte] = {
    checkFileSize()
    FileUtils.readFileBytes(canonicalPath)
  }

  // TODO: assess whether it is okay to link instead of copy
  override protected def localizeTo(file: Path): Unit = {
    if (canonicalPath == file) {
      logger.trace(
          s"Skipping 'download' of local file ${canonicalPath} - source and dest paths are equal"
      )
    } else if (isDirectory) {
      if (Files.isDirectory(canonicalPath)) {
        logger.trace(s"Copying directory ${canonicalPath} to ${file}")
        FileUtils.copyDirectory(canonicalPath, file)
      } else {
        logger.trace(s"Unpacking archive ${canonicalPath} to ${file}")
        FileUtils.unpackArchive(canonicalPath, file)
      }
    } else {
      logger.trace(s"Copying file ${canonicalPath} to ${file}")
      checkExists(true)
      Files.copy(canonicalPath, file)
    }
  }

  private class ListingFileVisitor extends SimpleFileVisitor[Path] {
    private var dirs = Map.empty[Path, Vector[Path]]

    override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
      // add an entry for this directory to the map, and if we've already seen the parent,
      // add this directory to its children
      dirs ++= Vector(
          Option(dir.getParent).flatMap(p => dirs.get(p).map(p -> _)).map {
            case (parent, children) => parent -> (children :+ dir)
          },
          Some(dir -> Vector.empty[Path])
      ).flatten.toMap
      FileVisitResult.CONTINUE
    }

    override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
      // add the file to its parent directory's children
      val parent = file.getParent
      dirs += (parent -> (dirs(parent) :+ file))
      FileVisitResult.CONTINUE
    }

    def buildListing(parent: LocalFileSource): Option[Vector[FileSource]] = {
      dirs.get(parent.canonicalPath).map { children =>
        children.map {
          case dir if Files.isDirectory(dir) =>
            val dirSource =
              LocalFileSource(LocalFileSource.resolve(dir), encoding, isDirectory = true)(
                  dir.toUri.toString,
                  dir,
                  cachedParent = Some(parent),
                  logger = logger
              )
            dirSource.cachedListing = buildListing(dirSource)
            dirSource
          case file =>
            LocalFileSource(LocalFileSource.resolve(file), encoding, isDirectory = false)(
                file.toUri.toString,
                file,
                cachedParent = Some(parent),
                logger = logger
            )
        }
      }
    }
  }

  override def listing(recursive: Boolean = false): Vector[FileSource] = {
    cachedListing
      .orElse(
          Option
            .when(isDirectory && exists) {
              val visitor = new ListingFileVisitor
              val maxDepth = if (recursive) Integer.MAX_VALUE else 1
              Files.walkFileTree(canonicalPath,
                                 javautil.EnumSet.noneOf(classOf[FileVisitOption]),
                                 maxDepth,
                                 visitor)
              visitor.buildListing(this)
            }
            .flatten
      )
      .getOrElse(throw new UnsupportedOperationException())
  }
}

object LocalFileSource {
  // search for a relative path in the directories of `searchPath`
  private def findInPath(relPath: String, searchPath: Vector[Path]): Option[Path] = {
    searchPath
      .map(d => d.resolve(relPath))
      .collectFirst {
        case fp if Files.exists(fp) => fp.toRealPath()
      }
  }

  def resolve(path: Path, searchPath: Vector[Path] = Vector.empty): Path = {
    if (Files.exists(path)) {
      path.toRealPath()
    } else if (path.isAbsolute) {
      FileUtils.normalizePath(path)
    } else {
      findInPath(path.toString, searchPath).getOrElse(
          // it's a non-existant relative path - localize it to current working dir
          FileUtils.absolutePath(path)
      )
    }
  }
}

case class LocalFileAccessProtocol(searchPath: Vector[Path] = Vector.empty,
                                   logger: Logger = Logger.Quiet,
                                   encoding: Charset = FileUtils.DefaultEncoding)
    extends FileAccessProtocol {
  override val schemes = Vector("", FileUtils.FileScheme)
  override val supportsDirectories: Boolean = true

  private def addressToPath(address: String): Path = {
    getUriScheme(address) match {
      case Some(FileScheme) => Paths.get(URI.create(address))
      case None             => FileUtils.getPath(address)
      case _                => throw new Exception(s"${address} is not a path or file:// URI")
    }
  }

  def resolvePath(path: Path,
                  value: Option[String] = None,
                  isDirectory: Option[Boolean] = None): LocalFileSource = {
    val resolved = LocalFileSource.resolve(path, searchPath)
    val isDir = isDirectory
      .orElse(Option.when(Files.exists(resolved))(Files.isDirectory(resolved)))
      .getOrElse(
          throw new Exception("'isDirectory' must be specified for a non-existing path")
      )
    LocalFileSource(resolved, encoding, isDir)(value.getOrElse(path.toString),
                                               path,
                                               logger = logger)
  }

  def resolve(address: String): LocalFileSource = {
    resolvePath(addressToPath(address), Some(address), isDirectory = Some(false))
  }

  override def resolveDirectory(address: String): LocalFileSource = {
    resolvePath(addressToPath(address), Some(address), isDirectory = Some(true))
  }
}

case class HttpFileSource(
    override val uri: URI,
    override val encoding: Charset,
    override val isDirectory: Boolean
)(override val address: String)
    extends AbstractAddressableFileNode(address, encoding) {

  private lazy val path = FileUtils.getPath(uri.getPath)

  override lazy val name: String = path.getFileName.toString

  override lazy val folder: String = path.getParent match {
    case null   => ""
    case parent => parent.toString
  }

  override def container: String = s"${uri.getScheme}:${uri.getHost}:${folder}"

  private var hasBytes: Boolean = false

  private def withConnection[T](fn: HttpURLConnection => T): T = {
    val url = uri.toURL
    var conn: HttpURLConnection = null
    try {
      conn = url.openConnection().asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("HEAD")
      fn(conn)
    } finally {
      if (conn != null) {
        conn.disconnect()
      }
    }
  }

  override def exists: Boolean = {
    try {
      val rc = withConnection(conn => conn.getResponseCode)
      rc == HttpURLConnection.HTTP_OK
    } catch {
      case _: Throwable => false
    }
  }

  override def getParent: Option[HttpFileSource] = {
    if (path.getParent == null) {
      None
    } else {
      val newUri = if (isDirectory) {
        uri.resolve("..")
      } else {
        uri.resolve(".")
      }
      Some(HttpFileSource(newUri, encoding, isDirectory = true)(newUri.toString))
    }
  }

  override def resolve(path: String): HttpFileSource = {
    val newUri = if (isDirectory) {
      uri.resolve(path)
    } else {
      uri.resolve(".").resolve(path)
    }
    HttpFileSource(newUri, encoding, path.endsWith("/"))(newUri.toString)
  }

  override def relativize(fileSource: AddressableFileSource): String = {
    fileSource match {
      case fs: HttpFileSource if isDirectory =>
        path.relativize(fs.path).toString
      case fs: HttpFileSource =>
        path.getParent.relativize(fs.path).toString
      case _ =>
        throw new Exception(s"not a HttpFileSource: ${fileSource}")
    }
  }

  // https://stackoverflow.com/questions/12800588/how-to-calculate-a-file-size-from-url-in-java
  override lazy val size: Long = {
    try {
      withConnection(conn => conn.getContentLengthLong)
    } catch {
      case t: Throwable =>
        throw new Exception(s"Error getting size of URL ${uri}: ${t.getMessage}")
    }
  }

  private def fetchUri(buffer: OutputStream, chunkSize: Int = 16384): Int = {
    val url = uri.toURL
    val is = url.openStream()
    try {
      // read all the bytes from the URL
      var nRead = 0
      var totalRead = 0
      val data = new Array[Byte](chunkSize)
      do {
        nRead = is.read(data, 0, chunkSize)
        if (nRead > 0) {
          buffer.write(data, 0, nRead)
          totalRead += nRead
        }
      } while (nRead > 0)
      totalRead
    } finally {
      is.close()
    }
  }

  override lazy val readBytes: Array[Byte] = {
    checkFileSize()
    val buffer = new ByteArrayOutputStream()
    try {
      fetchUri(buffer)
      hasBytes = true
      buffer.toByteArray
    } finally {
      buffer.close()
    }
  }

  private def localizeToFile(path: Path): Unit = {
    // avoid re-downloading the file if we've already cached the bytes
    if (hasBytes) {
      FileUtils.writeFileContent(path, new String(readBytes, encoding))
    } else {
      val buffer = new FileOutputStream(path.toFile)
      try {
        fetchUri(buffer)
      } finally {
        buffer.close()
      }
    }
  }

  override protected def localizeTo(file: Path): Unit = {
    if (isDirectory) {
      // localize to a temp file if this is a "directory" (i.e. an archive we're going to unpack)
      val dest = Files.createTempFile("temp", name)
      try {
        localizeToFile(dest)
        // Unpack the archive and delete the temp file
        if (Files.exists(file)) {
          FileUtils.deleteRecursive(file)
        }
        FileUtils.unpackArchive(dest, file)
      } finally {
        FileUtils.deleteRecursive(dest)
      }
    } else {
      localizeToFile(file)
    }
  }

  override def isListable: Boolean = false
}

case class HttpFileAccessProtocol(encoding: Charset = FileUtils.DefaultEncoding)
    extends FileAccessProtocol {
  override val schemes = Vector(FileUtils.HttpScheme, FileUtils.HttpsScheme)
  // directories are supported via unpacking of archive files
  override val supportsDirectories: Boolean = true

  def resolve(uri: URI, value: Option[String] = None): HttpFileSource = {
    HttpFileSource(uri, encoding, isDirectory = false)(value.getOrElse(uri.toString))
  }

  override def resolve(address: String): HttpFileSource = {
    resolve(URI.create(address), Some(address))
  }

  // TODO: currently the only way to specify an http directory is as an
  //  archive file that will be unpacked when localized.
  //  HTTP does not have the concept of directory listings; though they
  //  may be supported by some serevers, there is no standard response
  //  unless the server supports WebDAV. Handling those results is
  //  probably outside the scope of this package.
  override def resolveDirectory(address: String): HttpFileSource = {
    HttpFileSource(URI.create(address), encoding, isDirectory = true)(address)
  }
}

case class FileSourceResolver(protocols: Vector[FileAccessProtocol]) {
  sys.addShutdownHook({
    protocols.foreach { protocol =>
      try {
        protocol.onExit()
      } catch {
        case ex: Throwable =>
          Logger.error(s"Error shutting down protocol ${protocol}", Some(ex))
      }
    }
  })

  private lazy val protocolMap: Map[String, FileAccessProtocol] =
    protocols.flatMap(prot => prot.schemes.map(prefix => prefix -> prot)).toMap

  def canResolve(scheme: String): Boolean = {
    protocolMap.contains(scheme)
  }

  def getProtocolForScheme(scheme: String): FileAccessProtocol = {
    protocolMap.get(scheme) match {
      case None        => throw NoSuchProtocolException(scheme)
      case Some(proto) => proto
    }
  }

  def resolve(address: String,
              parent: Option[AddressableFileSource] = None): AddressableFileNode = {
    FileUtils.getUriScheme(address) match {
      case Some(scheme) =>
        // a full URI
        getProtocolForScheme(scheme).resolve(address)
      case None if parent.isDefined =>
        // a relative path - try to resolve against the parent
        parent.get.resolve(address) match {
          case fn: AddressableFileNode if fn.exists =>
            // a path relative to parent
            fn
          case _: LocalFileSource =>
            // the imported file is not relative to the parent, but
            // but LocalFileAccessProtocol may be configured to look
            // for it in a different folder
            fromFile(FileUtils.getPath(address))
          case other =>
            throw new Exception(s"Not an AddressableFileNode: ${other}")
        }
      case None =>
        getProtocolForScheme(FileUtils.FileScheme).resolve(address)
    }
  }

  def resolveDirectory(address: String,
                       parent: Option[AddressableFileSource] = None): AddressableFileSource = {
    FileUtils.getUriScheme(address) match {
      case Some(scheme) =>
        val proto = getProtocolForScheme(scheme)
        if (!proto.supportsDirectories) {
          throw ProtocolFeatureNotSupportedException(scheme, "directories")
        }
        proto.resolveDirectory(address)
      case None if parent.isDefined =>
        parent.get.resolve(address) match {
          case fs: AddressableFileSource if fs.exists =>
            // a path relative to parent
            fs
          case _: LocalFileSource =>
            // the imported file is not relative to the parent, but
            // but LocalFileAccessProtocol may be configured to look
            // for it in a different folder
            fromFile(FileUtils.getPath(address))
          case other =>
            throw new Exception(s"Not an AddressableFileNode: ${other}")
        }
      case None =>
        getProtocolForScheme(FileUtils.FileScheme).resolveDirectory(address)
    }
  }

  def fromPath(path: Path,
               isDirectory: Option[Boolean] = None,
               parent: Option[LocalFileSource] = None): LocalFileSource = {
    if (parent.isDefined && !path.isAbsolute) {
      parent.get.resolve(path.toString)
    } else {
      getProtocolForScheme(FileUtils.FileScheme) match {
        case proto: LocalFileAccessProtocol =>
          proto.resolvePath(path, isDirectory = isDirectory)
        case other =>
          throw new RuntimeException(s"Expected LocalFileAccessProtocol not ${other}")
      }
    }
  }

  def fromFile(path: Path, parent: Option[LocalFileSource] = None): LocalFileSource = {
    fromPath(path, isDirectory = Some(false), parent)
  }

  def localSearchPath: Vector[Path] = {
    protocols
      .collectFirst {
        case LocalFileAccessProtocol(searchPath, _, _) => searchPath
      }
      .getOrElse(Vector.empty)
  }

  def addToLocalSearchPath(paths: Vector[Path], append: Boolean = true): FileSourceResolver = {
    val newProtos = protocols.map {
      case LocalFileAccessProtocol(searchPath, logger, encoding) =>
        val newSearchPath = if (append) searchPath ++ paths else paths ++ searchPath
        LocalFileAccessProtocol(newSearchPath, logger, encoding)
      case other => other
    }
    FileSourceResolver(newProtos)
  }

  def replaceProtocol[T <: FileAccessProtocol](
      newProtocol: T
  )(implicit tag: ClassTag[T]): FileSourceResolver = {
    val newProtos = protocols.map {
      case _: T  => newProtocol
      case other => other
    }
    FileSourceResolver(newProtos)
  }
}

object FileSourceResolver {
  private var instance: Option[FileSourceResolver] = None

  def get: FileSourceResolver = {
    instance.getOrElse({
      val instance = create()
      set(instance)
      instance
    })
  }

  def set(fileResolver: FileSourceResolver): Option[FileSourceResolver] = {
    val currentInstance = instance
    instance = Some(fileResolver)
    currentInstance
  }

  def set(localDirectories: Vector[Path] = Vector.empty,
          userProtocols: Vector[FileAccessProtocol] = Vector.empty,
          logger: Logger = Logger.get,
          encoding: Charset = FileUtils.DefaultEncoding): Option[FileSourceResolver] = {
    set(create(localDirectories, userProtocols, logger, encoding))
  }

  def create(localDirectories: Vector[Path] = Vector.empty,
             userProtocols: Vector[FileAccessProtocol] = Vector.empty,
             logger: Logger = Logger.Quiet,
             encoding: Charset = FileUtils.DefaultEncoding): FileSourceResolver = {
    val protocols: Vector[FileAccessProtocol] = Vector(
        LocalFileAccessProtocol(localDirectories, logger, encoding),
        HttpFileAccessProtocol(encoding)
    )
    FileSourceResolver(protocols ++ userProtocols)
  }

  def getScheme(address: String): String = {
    FileUtils.getUriScheme(address).getOrElse(FileUtils.FileScheme)
  }
}

/**
  * A VirtualFileNode only exists in memory. It cannot be resolved.
  * @param name node name
  * @param encoding character encoding
  */
abstract class AbstractVirtualFileNode(override val name: String,
                                       val encoding: Charset = FileUtils.DefaultEncoding)
    extends FileNode {

  override lazy val toString: String = name

  /**
    * Whether the file described by this FileSource exists.
    */
  override val exists: Boolean = true

  override lazy val readBytes: Array[Byte] = readString.getBytes(encoding)

  override protected def localizeTo(file: Path): Unit = {
    FileUtils.writeFileContent(file, readString)
  }
}

case class StringFileNode(contents: String,
                          override val name: String = "<string>",
                          override val encoding: Charset = FileUtils.DefaultEncoding)
    extends AbstractVirtualFileNode(name, encoding) {
  override def readString: String = contents

  lazy val readLines: Vector[String] = {
    Source.fromString(contents).getLines().toVector
  }
}

object StringFileNode {
  def withName(name: String, contents: String): StringFileNode = {
    StringFileNode(contents, name)
  }

  lazy val empty: StringFileNode = StringFileNode("")
}

case class LinesFileNode(override val readLines: Vector[String],
                         override val name: String = "<lines>",
                         override val encoding: Charset = FileUtils.DefaultEncoding,
                         lineSeparator: String = "\n",
                         trailingNewline: Boolean = true)
    extends AbstractVirtualFileNode(name, encoding) {

  override def readString: String =
    FileUtils.linesToString(readLines, lineSeparator, trailingNewline)
}

object LinesFileNode {
  def withName(name: String, lines: Vector[String]): LinesFileNode = {
    LinesFileNode(lines, name)
  }
}
