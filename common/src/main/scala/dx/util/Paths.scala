package dx.util

import java.nio.file.{Files, Path}

import spray.json.{JsString, JsValue}

trait EvalPaths {
  def getRootDir(ensureExists: Boolean = false): Path

  def getTempDir(ensureExists: Boolean = false): Path

  /**
    * The execution directory - used as the base dir for relative paths (e.g. for glob search).
    */
  def getWorkDir(ensureExists: Boolean = false): Path

  def getMetaDir(ensureExists: Boolean = false): Path

  /**
    * The file that has a copy of standard output.
    */
  def getStdoutFile(ensureParentExists: Boolean = false): Path

  /**
    * The file that has a copy of standard error.
    */
  def getStderrFile(ensureParentExists: Boolean = false): Path
}

abstract class BaseEvalPaths extends EvalPaths {
  private var cache: Map[String, Path] = Map.empty

  protected def createDir(key: String, path: Path): Path = {
    val resolved = FileUtils.createDirectories(path)
    cache += (key -> resolved)
    resolved
  }

  protected def getOrCreateDir(key: String, path: Path, ensureExists: Boolean): Path = {
    val resolved = cache.getOrElse(key, if (ensureExists) createDir(key, path) else path)
    if (Files.exists(resolved)) {
      resolved.toRealPath()
    } else {
      resolved.toAbsolutePath
    }
  }
}

trait ExecPaths extends EvalPaths {
  def getCommandFile(ensureParentExists: Boolean = false): Path

  def getReturnCodeFile(ensureParentExists: Boolean = false): Path

  def getContainerCommandFile(ensureParentExists: Boolean = false): Path

  def getContainerIdFile(ensureParentExists: Boolean = false): Path

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
        if (!onlyExisting || Files.exists(path)) {
          Some(key -> JsString(path.toString))
        } else {
          None
        }
    }
  }
}
