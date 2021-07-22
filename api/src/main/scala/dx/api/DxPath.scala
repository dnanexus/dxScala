package dx.api

import dx.util.FileUtils

import java.net.{URI, URLDecoder}

object DxPath {
  val DxScheme = "dx"
  val DxUriPrefix = s"${DxScheme}://"
  private val pathRegex = "(.*)/(.+)".r

  def split(dxPath: String): (Option[String], String) = {
    // strip the prefix
    val s = if (dxPath.startsWith(DxUriPrefix)) {
      dxPath.substring(DxUriPrefix.length)
    } else {
      dxPath
    }

    // take out the project, if it is specified
    val components = s.split(":").toList
    val (projName, dxObjectPath) = components match {
      case Nil           => throw new Exception(s"Path ${dxPath} is invalid")
      case List(objName) => (None, objName)
      case projName :: tail =>
        val rest = tail.mkString(":")
        (Some(projName), rest)
    }

    projName match {
      case Some(proj) if proj.startsWith("file-") =>
        throw new Exception("""|Path ${dxPath} does not look like: dx://PROJECT_NAME:/FILE_PATH
                               |For example:
                               |   dx://dxCompiler_playground:/test_data/fileB
                               |""".stripMargin)
      case _ => ()
    }

    (projName, dxObjectPath)
  }

  case class DxPathComponents(name: String,
                              folder: Option[String],
                              projName: Option[String],
                              objFullName: String,
                              sourcePath: String)

  def parse(dxPath: String): DxPathComponents = {
    val enc = FileUtils.DefaultEncoding.name
    val (projName, dxObjectPath) = split(URLDecoder.decode(dxPath, enc))

    val (folder, name) = dxObjectPath match {
      case pathRegex(_, name) if DxUtils.isDataObjectId(name) => (None, name)
      case pathRegex(folder, name) if folder == ""            => (Some("/"), name)
      case pathRegex(folder, name)                            => (Some(FileUtils.getPath(folder).toString), name)
      case _ if DxUtils.isDataObjectId(dxObjectPath)          => (None, dxObjectPath)
      case _                                                  => (Some("/"), dxObjectPath)
    }

    DxPathComponents(name, folder, projName, dxObjectPath, dxPath)
  }

  /**
    * Formats a file ID with optional project name or ID as a dx:// URI.
    */
  def format(fileId: String, project: Option[String]): String = {
    project
      .map(proj => new URI(DxScheme, s"${proj}:${fileId}", null, null, null).toString)
      .getOrElse(s"${DxUriPrefix}${fileId}")
  }

  /**
    * Formats a project and file path to a dx:// URI.
    */
  def format(project: String, folder: String, name: String): String = {
    val path = FileUtils.getPath(folder).resolve(name).toString
    new URI(DxScheme, s"${project}:", path, null, null).toString
  }
}
