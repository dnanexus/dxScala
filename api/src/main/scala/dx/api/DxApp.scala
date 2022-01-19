package dx.api

import dx.AppInternalException
import spray.json._

case class DxAppDescribe(id: String,
                         name: String,
                         version: String,
                         created: Long,
                         modified: Long,
                         tags: Option[Set[String]],
                         properties: Option[Map[String, String]],
                         details: Option[JsValue],
                         inputSpec: Option[Vector[IOParameter]],
                         outputSpec: Option[Vector[IOParameter]],
                         access: Option[JsValue] = None)
    extends DxObjectDescribe {
  override def containsAll(fields: Set[Field.Value]): Boolean = {
    fields.diff(DxAppDescribe.RequiredFields).forall {
      case Field.Properties => properties.isDefined
      case Field.Details    => details.isDefined
      case Field.InputSpec  => inputSpec.isDefined
      case Field.OutputSpec => outputSpec.isDefined
      case Field.Access     => access.isDefined
      case _                => false
    }
  }
}

object DxAppDescribe {
  val RequiredFields = Set(Field.Id, Field.Name, Field.Version, Field.Created, Field.Modified)
  val DefaultFields: Set[Field.Value] = RequiredFields ++ Set(Field.InputSpec, Field.OutputSpec)
}

case class DxApp(id: String)(dxApi: DxApi = DxApi.get)
    extends CachingDxObject[DxAppDescribe]
    with DxExecutable {
  override def describeNoCache(fields: Set[Field.Value] = Set.empty): DxAppDescribe = {
    val allFields = fields ++ DxAppDescribe.DefaultFields
    val descJs = dxApi.appDescribe(id, Map("fields" -> DxObject.requestFields(allFields)))
    DxApp.parseDescribeJson(descJs, dxApi)
  }

  def newRun(name: String,
             input: JsValue,
             instanceType: Option[String] = None,
             details: Option[JsValue] = None,
             delayWorkspaceDestruction: Option[Boolean] = None,
             folder: Option[String] = None,
             priority: Option[Priority.Priority] = None): DxJob = {
    val fields = Map(
        "name" -> JsString(name),
        "input" -> input
    )
    // If this is a task that specifies the instance type
    // at runtime, launch it in the requested instance.
    val instanceFields = instanceType match {
      case None => Map.empty
      case Some(iType) =>
        Map(
            "systemRequirements" -> JsObject(
                "main" -> JsObject("instanceType" -> JsString(iType))
            )
        )
    }
    val detailsFields = details match {
      case Some(jsv) => Map("details" -> jsv)
      case None      => Map.empty
    }
    val dwd = delayWorkspaceDestruction match {
      case Some(true) => Map("delayWorkspaceDestruction" -> JsTrue)
      case _          => Map.empty
    }
    val folderFields = folder match {
      case Some(folder) => Map("folder" -> JsString(folder))
      case None         => Map.empty
    }
    val priorityFields = priority match {
      case Some(priority) => Map("priority" -> JsString(priority.toString.toLowerCase))
      case None           => Map.empty
    }
    val info =
      dxApi.appRun(
          this.id,
          fields ++ instanceFields ++ detailsFields ++ dwd ++ folderFields ++ priorityFields
      )
    val id: String = info.fields.get("id") match {
      case Some(JsString(x)) => x
      case _ =>
        throw new AppInternalException(s"Bad format returned from jobNew ${info.prettyPrint}")
    }
    dxApi.job(id)
  }
}

object DxApp {
  def parseDescribeJson(descJs: JsObject, dxApi: DxApi): DxAppDescribe = {
    val desc =
      descJs
        .getFields("id",
                   "name",
                   "version",
                   "created",
                   "modified",
                   "inputSpec",
                   "outputSpec") match {
        case Seq(JsString(id),
                 JsString(name),
                 JsString(version),
                 JsNumber(created),
                 JsNumber(modified),
                 JsArray(inputSpec),
                 JsArray(outputSpec)) =>
          DxAppDescribe(
              id,
              name,
              version,
              created.toLong,
              modified.toLong,
              None,
              None,
              None,
              Some(IOParameter.parseIOSpec(dxApi, inputSpec)),
              Some(IOParameter.parseIOSpec(dxApi, outputSpec))
          )
        case _ =>
          throw new Exception(s"Malformed JSON ${descJs}")
      }
    val details = descJs.fields.get("details")
    val tags = descJs.fields.get("tags").map(DxObject.parseJsonTags)
    val properties = descJs.fields.get("properties").map(DxObject.parseJsonProperties)
    val access = descJs.fields.get("access")
    desc.copy(details = details, tags = tags, properties = properties, access = access)
  }
}
