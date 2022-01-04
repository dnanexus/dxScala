package dx.util

import java.nio.file.Files
import spray.json.{JsString, JsValue}

trait EvalPaths {
  def getRootDir(ensureExists: Boolean = false): PosixPath

  def getTempDir(ensureExists: Boolean = false): PosixPath

  /**
    * The execution directory - used as the base dir for relative paths (e.g. for glob search).
    */
  def getWorkDir(ensureExists: Boolean = false): PosixPath

  def getMetaDir(ensureExists: Boolean = false): PosixPath

  /**
    * The file that has a copy of standard output.
    */
  def getStdoutFile(ensureParentExists: Boolean = false): PosixPath

  /**
    * The file that has a copy of standard error.
    */
  def getStderrFile(ensureParentExists: Boolean = false): PosixPath
}

abstract class BaseEvalPaths(isLocal: Boolean = true) extends EvalPaths {
  private var cache: Map[String, PosixPath] = Map.empty

  protected def createDir(key: String, path: PosixPath): PosixPath = {
    val resolved = PosixPath(FileUtils.createDirectories(path.asJavaPath).toString)
    cache += (key -> resolved)
    resolved
  }

  protected def getOrCreateDir(key: String, path: PosixPath, ensureExists: Boolean): PosixPath = {
    cache.getOrElse(
        key,
        if (!isLocal) {
          path
        } else if (ensureExists) {
          createDir(key, path)
        } else {
          val localPath = path.asJavaPath
          if (Files.exists(localPath)) {
            PosixPath(localPath.toRealPath().toString)
          } else {
            path
          }
        }
    )
  }
}

trait ExecPaths extends EvalPaths {
  def getCommandFile(ensureParentExists: Boolean = false): PosixPath

  def getReturnCodeFile(ensureParentExists: Boolean = false): PosixPath

  def getContainerCommandFile(ensureParentExists: Boolean = false): PosixPath

  def getContainerIdFile(ensureParentExists: Boolean = false): PosixPath

  def toJson(onlyExisting: Boolean = true): Map[String, JsValue] = {
    Map(
        "root" -> getRootDir(),
        "work" -> getWorkDir(),
        "meta" -> getMetaDir(),
        "tmp" -> getTempDir(),
        "stdout" -> getStdoutFile(),
        "stderr" -> getStderrFile(),
        "commands" -> getCommandFile(),
        "returnCode" -> getReturnCodeFile(),
        "containerCommands" -> getContainerCommandFile(),
        "containerId" -> getContainerIdFile()
    ).flatMap {
      case (key, path) =>
        if (!onlyExisting || Files.exists(path.asJavaPath)) {
          Some(key -> JsString(path.toString))
        } else {
          None
        }
    }
  }
}
