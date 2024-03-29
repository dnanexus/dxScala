package dx.util

import java.io.{BufferedReader, File, FileNotFoundException, IOException, InputStreamReader}
import java.nio.charset.Charset
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{
  FileAlreadyExistsException,
  FileVisitResult,
  Files,
  Path,
  Paths,
  SimpleFileVisitor
}
import sun.security.action.GetPropertyAction

import scala.io.{Codec, Source}
import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

object FileUtils {
  val FileScheme: String = "file"
  val HttpScheme: String = "http"
  val HttpsScheme: String = "https"
  private val validSchemeRegex = "^([a-z]+?)$".r
  private val validPathRegex = "^((?:[A-Za-z0-9-._~:/?#\\[\\]@!$&'()*+,;=]|%[0-9A-F]{2})+)$".r
  // the spec states that WDL files must use UTF8 encoding
  val DefaultEncoding: Charset = Codec.UTF8.charSet
  val DefaultLineSeparator: String = "\n"

  def systemTempDir: Path = {
    try {
      getPath(GetPropertyAction.privilegedGetProperty("java.io.tmpdir"))
    } catch {
      case _: Throwable => Paths.get("/tmp")
    }
  }

  /**
    * Removes '.' and '..' elements from a path. Beginning '..'
    * elements of a relative path are preserved, otherwise a '..'
    * removes the preceeding path element.
    * @example
    *   /A/./B/../C -> /A/C
    *   ../foo/../bar -> ../bar
    *   ./hello.txt -> hello.txt
    */
  def normalizePath(path: Path): Path = {
    if (path.isAbsolute && Files.exists(path)) {
      path.toRealPath()
    } else {
      val elements = Option(path.getRoot).map(_.toString).toVector ++ path
        .iterator()
        .asScala
        .map(_.toString)
        .dropWhile(_ == ".")
      val filtered = elements.foldLeft(Vector.empty[String]) {
        case (accu, element) if element == "." => accu
        case (accu, element) if element == ".." && accu.lastOption.exists(_ != "..") =>
          accu.dropRight(1)
        case (accu, element) => accu :+ element
      }
      filtered match {
        case Vector()  => new File(".").toPath
        case Vector(p) => new File(p).toPath
        case v =>
          v.tail.foldLeft(new File(v.head).toPath) {
            case (parent, child) => parent.resolve(child)
          }
      }
    }
  }

  /**
    * Returns the absolute, normalized version of `path`.
    */
  def absolutePath(path: Path): Path = {
    normalizePath(path.toAbsolutePath)
  }

  /**
    * Returns the current working directory. If `absolute=true` this
    * is an absolute, normalized path, otherwise it is a relative path.
    */
  def cwd(absolute: Boolean = false): Path = {
    val relCwd = new File(".").toPath
    if (absolute) {
      absolutePath(relCwd)
    } else {
      relCwd
    }
  }

  /**
    * Converts a String to a Path. Use this instead of `Paths.get()` if you want a relative
    * path to remain relative (`Paths.get()` may convert a relative path to an absolute path
    * relative to cwd).
    * @param path the path string
    * @return
    */
  def getPath(path: String): Path = {
    normalizePath(new File(path).toPath)
  }

  implicit class RegexExtensions(regex: Regex) {
    def split(toSplit: CharSequence, limit: Int): Array[String] = {
      regex.pattern.split(toSplit, limit)
    }
  }

  def getUriScheme(pathOrUri: String): Option[String] = {
    pathOrUri.split(":/", 2).toVector match {
      case Vector(validSchemeRegex(scheme), validPathRegex(_)) => Some(scheme)
      case Vector(_)                                           => None
      case _ =>
        throw new Exception(s"URI contains invalid character: ${pathOrUri}")
    }
  }

  private val sanitizeRegexp = "[^A-Za-z0-9._-]".r

  def sanitizeFileName(fileName: String): String = {
    sanitizeRegexp.replaceAllIn(fileName, "_")
  }

  def changeFileExt(fileName: String, dropExt: String = "", addExt: String = ""): String = {
    ((fileName, dropExt) match {
      case (fn, ext) if ext.nonEmpty && fn.endsWith(ext) =>
        fn.dropRight(dropExt.length)
      case (_, ext) if ext.nonEmpty =>
        throw new Exception(s"${fileName} does not have extension ${dropExt}")
      case (fn, _) => fn
    }) + addExt
  }

  def changeFirstFileExt(fileName: String,
                         dropExts: Vector[String],
                         addExt: String = ""): String = {
    dropExts
      .collectFirst {
        case ext if fileName.endsWith(ext) => changeFileExt(fileName, ext, addExt)
      }
      .getOrElse(
          throw new Exception(s"${fileName} does not end with any of ${dropExts.mkString(",")}")
      )
  }

  def replaceFileSuffix(path: Path, suffix: String): String = {
    replaceFileSuffix(path.getFileName.toString, suffix)
  }

  // Add a suffix to a filename, before the regular suffix. For example:
  //  xxx.wdl -> xxx.simplified.wdl
  def replaceFileSuffix(fileName: String, suffix: String): String = {
    val index = fileName.lastIndexOf('.')
    val prefix = if (index >= 0) {
      fileName.substring(0, index)
    } else {
      ""
    }
    changeFileExt(prefix, addExt = suffix)
  }

  def linesToString(lines: IterableOnce[String],
                    lineSeparator: String = DefaultLineSeparator,
                    trailingNewline: Boolean = false): String = {
    val s = lines.iterator.mkString(lineSeparator)
    if (trailingNewline) {
      s + lineSeparator
    } else {
      s
    }
  }

  def readStdinContent(encoding: Charset = DefaultEncoding): String = {
    if (System.in.available() == 0) {
      return ""
    }
    val in = new BufferedReader(new InputStreamReader(System.in, encoding))
    linesToString(Iterator.continually(in.readLine()).takeWhile(_ != null))
  }

  def readFileBytes(path: Path, mustExist: Boolean = true): Array[Byte] = {
    if (Files.exists(path)) {
      Files.readAllBytes(path)
    } else if (mustExist) {
      throw new FileNotFoundException(path.toString)
    } else {
      Array.emptyByteArray
    }
  }

  /**
    * Reads the entire contents of a file as a string. Line endings are not stripped or
    * converted.
    * @param path file path
    * @return file contents as a string
    */
  def readFileContent(path: Path,
                      encoding: Charset = DefaultEncoding,
                      mustExist: Boolean = true,
                      maxSize: Option[Long] = None): String = {
    maxSize.foreach { size =>
      if (path.toFile.length() > size) {
        throw new Exception(s"file ${path} is larger than ${maxSize} bytes")
      }
    }
    new String(readFileBytes(path, mustExist), encoding)
  }

  /**
    * Reads all the lines from a file and returns them as a Vector of strings. Lines have
    * line-ending characters ([\r\n]) stripped off. Notably, there is no way to know if
    * the last line originaly ended with a newline.
    * @param path the path to the file
    * @return a Seq of the lines from the file
    */
  def readFileLines(path: Path, encoding: Charset = DefaultEncoding): Vector[String] = {
    val source = Source.fromFile(path.toString, encoding.name)
    try {
      source.getLines().toVector
    } finally {
      source.close()
    }
  }

  def checkOverwrite(path: Path, overwrite: Boolean): Boolean = {
    if (Files.exists(path)) {
      if (!overwrite) {
        throw new FileAlreadyExistsException(s"${path} exists and overwrite = false")
      } else if (Files.isDirectory(path)) {
        throw new FileAlreadyExistsException(s"${path} already exists as a directory")
      }
      true
    } else {
      false
    }
  }

  /**
    * Write a String to a file.
    * @param content the string to write
    * @param path the path of the file
    * @param overwrite whether to overwrite an existing file
    * @param makeExecutable whether to set the file's executable flag
    */
  def writeFileContent(path: Path,
                       content: String,
                       overwrite: Boolean = true,
                       makeExecutable: Boolean = false): Path = {
    val absPath = absolutePath(path)
    checkOverwrite(absPath, overwrite)
    val parent = createDirectories(absPath.getParent)
    val resolved = parent.resolve(absPath.getFileName)
    Files.write(resolved, content.getBytes(DefaultEncoding))
    if (makeExecutable) {
      resolved.toFile.setExecutable(true)
    }
    resolved
  }

  /**
    * Copy a source file to a destination file/directory. If `linkOk` is true, and the
    * current system supports symplinks, creates a symlink rather than copying the file.
    * @param source the source file
    * @param dest the destination file
    * @param linkOk whether it is okay to create a symbolic link rather than copy the file
    * @return whether the result was a symlink (true) or copy (false)
    */
  def copyFile(source: Path,
               dest: Path,
               overwrite: Boolean = true,
               linkOk: Boolean = false): Boolean = {
    checkOverwrite(dest, overwrite)
    if (linkOk) {
      try {
        Files.createSymbolicLink(source, dest)
        return true
      } catch {
        case _: IOException => ()
      }
    }
    Files.copy(source, dest)
    false
  }

  /**
    * Files.createDirectories does not handle links. This function searches starting from dir to find the
    * first parent directory that exists, converts that to a real path, resolves the subdirectories, and
    * then creates them.
    * @param dir the directory path to create
    * @return the fully resolved and existing Path
    */
  def createDirectories(dir: Path): Path = {
    if (Files.exists(dir)) {
      if (Files.isDirectory(dir)) {
        return dir.toRealPath()
      } else {
        throw new FileAlreadyExistsException(dir.toString)
      }
    }
    var parent: Path = dir
    var subdirs: List[String] = List.empty
    while (parent != null && !Files.exists(parent)) {
      subdirs = parent.getFileName.toString :: subdirs
      parent = parent.getParent
    }
    if (parent == null) {
      throw new RuntimeException(s"None of the parents of ${dir} exist")
    }
    val realDir = Paths.get(parent.toRealPath().toString, subdirs: _*)
    Files.createDirectories(realDir)
    realDir
  }

  private case class CopyDirFileVisitor(sourceDir: Path, targetDir: Path)
      extends SimpleFileVisitor[Path] {
    override def visitFile(file: Path, attributes: BasicFileAttributes): FileVisitResult = {
      Files.copy(file, targetDir.resolve(sourceDir.relativize(file)))
      FileVisitResult.CONTINUE
    }

    override def preVisitDirectory(dir: Path, attributes: BasicFileAttributes): FileVisitResult = {
      Files.createDirectory(targetDir.resolve(sourceDir.relativize(dir)))
      FileVisitResult.CONTINUE
    }
  }

  /**
    * Copy a source directory to a destination directory. If `linkOk` is true, and the
    * current system supports symplinks, creates a symlink rather than copying the file.
    * @param sourceDir the source file
    * @param targetDir the destination file
    * @param linkOk whether it is okay to create a symbolic link rather than copy the file
    * @return whether the result was a symlink (true) or copy (false)
    */
  def copyDirectory(sourceDir: Path,
                    targetDir: Path,
                    overwrite: Boolean = true,
                    linkOk: Boolean = false): Boolean = {
    if (Files.exists(targetDir)) {
      if (overwrite) {
        deleteRecursive(targetDir)
      } else {
        throw new FileAlreadyExistsException(s"${targetDir} exists and overwrite = false")
      }
    }
    if (linkOk) {
      try {
        Files.createSymbolicLink(sourceDir, targetDir)
        return true
      } catch {
        case _: IOException => ()
      }
    }
    createDirectories(targetDir)
    Files.walkFileTree(sourceDir, CopyDirFileVisitor(sourceDir, targetDir))
    false
  }

  def deleteRecursive(path: Path): Unit = {
    if (Files.exists(path)) {
      if (Files.isDirectory(path)) {
        Files.walkFileTree(
            path.toRealPath(),
            new SimpleFileVisitor[Path] {
              override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
                Files.delete(file)
                FileVisitResult.CONTINUE
              }

              override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
                Files.delete(dir)
                FileVisitResult.CONTINUE
              }
            }
        )
      } else {
        Files.delete(path)
      }
    }
  }

  sealed abstract class ArchiveType(val name: String, val extensions: Set[String])

  object ArchiveType {
    case object Tar extends ArchiveType("tar", Set(".tar"))
    case object Tgz extends ArchiveType("tgz", Set(".tgz", ".tar.gz"))
    case object Zip extends ArchiveType("zip", Set(".zip"))

    val All = Vector(Tar, Tgz, Zip)

    def fromExtension(path: Path): ArchiveType = {
      val pathStr = path.getFileName.toString
      All
        .collectFirst {
          case a if a.extensions.exists(suffix => pathStr.endsWith(suffix)) => a
        }
        .getOrElse(
            throw new Exception(s"Not a supported archive file: ${path}")
        )
    }
  }

  /**
    * Unpack an archive file. For now we do this in a (platform-dependent) native manner, using
    * a subprocess to call the appropriate system tool based on the file extension.
    * @param archiveFile the archive file to unpack
    * @param targetDir the directory to which to unpack the file
    */
  def unpackArchive(archiveFile: Path,
                    targetDir: Path,
                    archiveType: Option[ArchiveType] = None,
                    overwrite: Boolean = true): Unit = {
    if (Files.exists(targetDir)) {
      if (overwrite) {
        deleteRecursive(targetDir)
      } else {
        throw new FileAlreadyExistsException(s"${targetDir} exists and overwrite = false")
      }
    }
    createDirectories(targetDir)
    val command = archiveType.getOrElse(ArchiveType.fromExtension(archiveFile)) match {
      case ArchiveType.Tar =>
        s"tar -xf '${archiveFile}' -C '${targetDir}'"
      case ArchiveType.Tgz =>
        s"tar -xzf '${archiveFile}' -C '${targetDir}'"
      case ArchiveType.Zip =>
        s"unzip '${archiveFile}' -d '${targetDir}'"
    }
    SysUtils.execCommand(command)
  }
}
