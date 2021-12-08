package dx.util

import java.nio.file.{FileSystems, Path}

object PosixPath {
  val Separator = "/"
  val Root: PosixPath = PosixPath(Vector.empty, isAbsolute = true)
  lazy val localFilesystemIsPosix: Boolean = FileSystems.getDefault.getSeparator == Separator

  def parse(path: String): (Vector[String], Boolean) = {
    val isAbsolute = path.startsWith(PosixPath.Separator)
    val parts = if (path == "") {
      Vector.empty
    } else {
      val parts = path.split(PosixPath.Separator).toVector
      assert(parts.nonEmpty)
      if (isAbsolute) {
        assert(parts(0) == "")
        parts.drop(1)
      } else {
        parts
      }
    }
    (parts, isAbsolute)
  }

  def apply(path: String): PosixPath = {
    val (parts, isAbsolute) = parse(path)
    PosixPath(parts, isAbsolute)
  }
}

/**
  * Similar to java.nio.Path but always parses/formats POSIX-style (i.e. with '/' path separator).
  */
case class PosixPath(parts: Vector[String], isAbsolute: Boolean) {
  def getName: Option[String] = parts.lastOption

  def getParent: Option[PosixPath] = {
    (parts.size, isAbsolute) match {
      case (0, _)     => None
      case (1, false) => None
      case (1, true)  => Some(PosixPath.Root)
      case _          => Some(PosixPath(parts.dropRight(1), isAbsolute))
    }
  }

  def resolve(path: String): PosixPath = {
    val (newParts, newIsAbsolute) = PosixPath.parse(path)
    if (newIsAbsolute) {
      PosixPath(newParts, newIsAbsolute)
    } else {
      PosixPath(parts ++ newParts, isAbsolute)
    }
  }

  def startsWith(other: PosixPath): Boolean = {
    isAbsolute == other.isAbsolute && parts.startsWith(other.parts)
  }

  def relativize(path: PosixPath): PosixPath = {
    if (!path.startsWith(this)) {
      throw new IllegalArgumentException(s"${path} does not start with ${this}")
    }
    PosixPath(path.parts.drop(parts.size), isAbsolute = false)
  }

  def asJavaPath: Path = {
    if (!PosixPath.localFilesystemIsPosix) {
      throw new Exception(s"cannot convert ${this} to a local path on a non-POSIX filesystem")
    }
    FileUtils.getPath(toString)
  }

  override def toString: String = {
    val names = parts.mkString(PosixPath.Separator)
    if (isAbsolute) {
      s"${PosixPath.Separator}${names}"
    } else {
      names
    }
  }
}
