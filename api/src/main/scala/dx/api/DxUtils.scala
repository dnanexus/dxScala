package dx.api

import dx.api.DxPath.DxScheme
import spray.json._

import java.net.URI

object DxUtils {
  val DxLinkKey = "$dnanexus_link"
  val RecordClass = "record"
  val RecordPrefix = s"${RecordClass}-"
  private val dataObjectClasses =
    Set("applet", "database", "dbcluster", "file", RecordClass, "workflow")
  private val containerClasses = Set("container", "project")
  private val executableClasses = Set("applet", "app", "globalworkflow", "workflow")
  private val executionClasses = Set("analysis", "job")
  private val allClasses = dataObjectClasses | containerClasses | executableClasses | executionClasses
  private val objectIdRegexp =
    s"^(${allClasses.mkString("|")})-([A-Za-z0-9]{24})$$".r
  private val dataObjectIdRegexp =
    s"^(${dataObjectClasses.mkString("|")})-([A-Za-z0-9]{24})$$".r
  private val executableIdRegexp =
    s"^(${executableClasses.mkString("|")})-([A-Za-z0-9]{24})$$".r
  // apps and globalworkflows can be referenced by name
  private val namedObjectIdRegexp =
    "^(app|globalworkflow)-([A-Za-z0-9._-]+)$".r
  // Other entity ID regexps if/when needed:
  //  private val containerIdRegexp = s"^(${containerClasses.mkString("|")})-(\\w{24})$$".r
  //  private val executableIdRegexp = s"^(${executableClasses.mkString("|")})-(\\w{24})$$".r
  //  private val executionIdRegexp = s"^(${executionClasses.mkString("|")})-(\\w{24})$$".r

  def parseObjectId(dxId: String): (String, String) = {
    dxId match {
      case objectIdRegexp(idType, idHash) =>
        (idType, idHash)
      case namedObjectIdRegexp(idType, idName) =>
        (idType, idName)
      case _ =>
        throw new IllegalArgumentException(s"${dxId} is not a valid object ID")
    }
  }

  def isObjectId(objName: String): Boolean = {
    try {
      parseObjectId(objName)
      true
    } catch {
      case _: IllegalArgumentException => false
    }
  }

  def parseDataObjectId(dxId: String): (String, String) = {
    dxId match {
      case dataObjectIdRegexp(idType, idHash) =>
        (idType, idHash)
      case _ =>
        throw new IllegalArgumentException(s"${dxId} is not a valid data object ID")
    }
  }

  def isDataObjectId(value: String): Boolean = {
    try {
      parseDataObjectId(value)
      true
    } catch {
      case _: IllegalArgumentException => false
    }
  }

  def isRecordId(objId: String): Boolean = {
    isDataObjectId(objId) && objId.startsWith(RecordPrefix)
  }

  def parseExecutableId(dxId: String): (String, String) = {
    dxId match {
      case executableIdRegexp(idType, idHash) =>
        (idType, idHash)
      case _ =>
        throw new IllegalArgumentException(s"${dxId} is not a valid data object ID")
    }
  }

  def dxDataObjectToUri(dxObj: DxDataObject): String = {
    dxObj match {
      case DxFile(_, Some(container)) =>
        s"${DxPath.DxUriPrefix}${container.id}:${dxObj.id}"
      case DxRecord(_, Some(container)) =>
        s"${DxPath.DxUriPrefix}${container.id}:${dxObj.id}"
      case _ =>
        s"${DxPath.DxUriPrefix}${dxObj.id}"
    }
  }

  def isLinkJson(jsv: JsValue): Boolean = {
    jsv match {
      case JsObject(fields) if fields.keySet == Set(DxLinkKey) => true
      case _                                                   => false
    }
  }

  // Create a dx link to a field in an execution. The execution could
  // be a job or an analysis.
  def dxExecutionToEbor(dxExec: DxExecution, fieldName: String): JsValue = {
    dxExec match {
      case _: DxJob =>
        JsObject(
            DxLinkKey -> JsObject("field" -> JsString(fieldName), "job" -> JsString(dxExec.id))
        )
      case _: DxAnalysis =>
        JsObject(
            DxLinkKey -> JsObject("field" -> JsString(fieldName), "analysis" -> JsString(dxExec.id))
        )
      case _ =>
        throw new Exception(s"Cannot create EBOR for execution ${dxExec.id}")
    }
  }

  def isEborJson(jsv: JsValue): Boolean = {
    jsv match {
      case JsObject(fields) if fields.keySet == Set(DxUtils.DxLinkKey) =>
        fields(DxUtils.DxLinkKey) match {
          case JsObject(fields2) if Set("job", "field").diff(fields2.keySet).isEmpty      => true
          case JsObject(fields2) if Set("analysis", "field").diff(fields2.keySet).isEmpty => true
          case _                                                                          => false
        }
      case _ => false
    }
  }

  /**
    * Formats a file ID and path with optional project to a dx:// URI.
    */
  def formatEbor(executableId: String, field: String, project: Option[String]): String = {
    val authority =
      project.map(proj => s"${proj}:${executableId}::").getOrElse(s"${executableId}::")
    new URI(DxScheme, authority, field, null, null).toString
  }

  def eborToUri(jsv: JsValue): String = {
    jsv match {
      case JsObject(fields) if fields.keySet == Set(DxUtils.DxLinkKey) =>
        val ebor = fields(DxUtils.DxLinkKey) match {
          case JsObject(ebor) => ebor
          case _              => throw new Exception(s"not an EBOR ${jsv}")
        }
        val JsString(id) = ebor
          .get("job")
          .orElse(ebor.get("analysis"))
          .getOrElse(
              throw new Exception(s"not an EBOR ${jsv}")
          )
        val JsString(field) = ebor("field")
        val project = ebor.get("project").map {
          case JsString(proj) => proj
          case other =>
            throw new Exception(s"invalid project value ${other}")
        }
        formatEbor(id, field, project)
      case _ => throw new Exception(s"not an EBOR ${jsv}")
    }
  }
}
