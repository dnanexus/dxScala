package dx.api

import spray.json._

case class DxAnalysisDescribe(project: String,
                              id: String,
                              name: String,
                              folder: String,
                              created: Long,
                              modified: Long,
                              executableName: Option[String],
                              properties: Option[Map[String, String]],
                              details: Option[JsValue],
                              input: Option[JsValue],
                              output: Option[JsValue],
                              dependsOn: Option[Vector[String]])
    extends DxObjectDescribe {
  override def containsAll(fields: Set[Field.Value]): Boolean = {
    fields.diff(DxAnalysisDescribe.DefaultFields).forall {
      case Field.ExecutableName => executableName.isDefined
      case Field.Properties     => properties.isDefined
      case Field.Details        => details.isDefined
      case Field.Input          => input.isDefined
      case Field.Output         => output.isDefined
      case Field.DependsOn      => dependsOn.isDefined
      case _                    => false
    }
  }
}

object DxAnalysisDescribe {
  val DefaultFields =
    Set(Field.Project, Field.Id, Field.Name, Field.Folder, Field.Created, Field.Modified)
}

case class DxAnalysis(id: String, project: Option[DxProject])(dxApi: DxApi = DxApi.get)
    extends CachingDxObject[DxAnalysisDescribe]
    with DxExecution {
  def describeNoCache(fields: Set[Field.Value] = Set.empty): DxAnalysisDescribe = {
    val projSpec = DxObject.maybeSpecifyProject(project)
    val allFields = fields ++ DxAnalysisDescribe.DefaultFields
    val request = projSpec + ("fields" -> DxObject.requestFields(allFields))
    val descJs = dxApi.analysisDescribe(id, request)
    DxAnalysis.parseDescribeJson(descJs)
  }

  def setProperties(props: Map[String, String]): Unit = {
    val request = Map(
        "properties" -> JsObject(props.view.mapValues(s => JsString(s)).toMap)
    )
    dxApi.analysisSetProperties(id, request)
  }
}

object DxAnalysis {
  def parseDescribeJson(descJs: JsObject): DxAnalysisDescribe = {
    val desc = descJs.getFields("project", "id", "name", "folder", "created", "modified") match {
      case Seq(JsString(project),
               JsString(id),
               JsString(name),
               JsString(folder),
               JsNumber(created),
               JsNumber(modified)) =>
        DxAnalysisDescribe(project,
                           id,
                           name,
                           folder,
                           created.toLong,
                           modified.toLong,
                           None,
                           None,
                           None,
                           None,
                           None,
                           None)
      case _ =>
        throw new Exception(s"Malformed JSON ${descJs}")
    }

    // optional fields
    val executableName = descJs.fields.get("executableName") match {
      case Some(JsString(x))   => Some(x)
      case Some(JsNull) | None => None
      case Some(other)         => throw new Exception(s"Invalid executable name ${other}")
    }
    val details = descJs.fields.get("details")
    val props = descJs.fields.get("properties").map(DxObject.parseJsonProperties)
    val input = descJs.fields.get("input")
    val output = descJs.fields.get("output")
    val dependsOn = descJs.fields.get("dependsOn") match {
      case Some(JsArray(deps)) =>
        Some(deps.map {
          case JsString(dep) => dep
          case other         => throw new Exception(s"invalid dependsOn item ${other}")
        })
      case None => None
      case other =>
        throw new Exception(s"invalid dependsOn value ${other}")
    }

    desc.copy(executableName = executableName,
              details = details,
              properties = props,
              input = input,
              output = output,
              dependsOn = dependsOn)
  }
}
