package dx.api

import spray.json._

case class DxDbclusterDescribe(project: String,
                               id: String,
                               name: String,
                               folder: String,
                               created: Long,
                               modified: Long,
                               properties: Option[Map[String, String]],
                               details: Option[JsValue],
                               status: String,
                               port: Long,
                               engineVersion: String,
                               engine: String,
                               dxInstanceClass: String,
                               statusAsOf: Long,
                               endpoint: String)
    extends DxObjectDescribe

case class DxDbcluster(id: String, project: Option[DxProject])(dxApi: DxApi = DxApi.get)
    extends DxDataObject {
  def describe(fields: Set[Field.Value] = Set.empty): DxDbclusterDescribe = {
    val projSpec = DxObject.maybeSpecifyProject(project)
    val defaultFields =
      Set(Field.Project, Field.Id, Field.Name, Field.Folder, Field.Created, Field.Modified)
    val dbclusterSpecificFields = Set(Field.Status,
                                      Field.Port,
                                      Field.EngineVersion,
                                      Field.Engine,
                                      Field.DxInstanceClass,
                                      Field.StatusAsOf,
                                      Field.Endpoint)
    val allFields = fields ++ defaultFields ++ dbclusterSpecificFields
    val descJs =
      dxApi.dbclusterDescribe(id, projSpec + ("fields" -> DxObject.requestFields(allFields)))
    val desc =
      descJs.getFields("project",
                       "id",
                       "name",
                       "folder",
                       "created",
                       "modified",
                       "status",
                       "port",
                       "engineVersion",
                       "engine",
                       "dxInstanceClass",
                       "statusAsOf",
                       "endpoint") match {
        case Seq(JsString(projectId),
                 JsString(id),
                 JsString(name),
                 JsString(folder),
                 JsNumber(created),
                 JsNumber(modified),
                 JsString(status),
                 JsNumber(port),
                 JsString(engineVersion),
                 JsString(engine),
                 JsString(dxInstanceClass),
                 JsNumber(statusAsOf),
                 JsString(endpoint)) =>
          DxDbclusterDescribe(projectId,
                              id,
                              name,
                              folder,
                              created.toLong,
                              modified.toLong,
                              None,
                              None,
                              status,
                              port.toLong,
                              engineVersion,
                              engine,
                              dxInstanceClass,
                              statusAsOf.toLong,
                              endpoint)
      }

    val details = descJs.fields.get("details")
    val props = descJs.fields.get("properties").map(DxObject.parseJsonProperties)
    desc.copy(details = details, properties = props)
  }
}
