package dx.api

import spray.json._

case class DxDatabaseDescribe(project: String,
                              id: String,
                              name: String,
                              folder: String,
                              created: Long,
                              modified: Long,
                              properties: Option[Map[String, String]],
                              details: Option[JsValue],
                              databaseName: String,
                              uniqueDatabaseName: String)
    extends DxObjectDescribe

case class DxDatabase(id: String, project: Option[DxProject])(dxApi: DxApi = DxApi.get)
    extends DxDataObject {
  def describe(fields: Set[Field.Value] = Set.empty): DxDatabaseDescribe = {
    val projSpec = DxObject.maybeSpecifyProject(project)
    val defaultFields =
      Set(Field.Project, Field.Id, Field.Name, Field.Folder, Field.Created, Field.Modified)
    val databaseSpecificFields = Set(Field.DatabaseName, Field.UniqueDatabaseName)
    val allFields = fields ++ defaultFields ++ databaseSpecificFields
    val descJs =
      dxApi.databaseDescribe(id, projSpec + ("fields" -> DxObject.requestFields(allFields)))
    val desc =
      descJs.getFields("project",
                       "id",
                       "name",
                       "folder",
                       "created",
                       "modified",
                       "databaseName",
                       "uniqueDatabaseName") match {
        case Seq(JsString(projectId),
                 JsString(id),
                 JsString(name),
                 JsString(folder),
                 JsNumber(created),
                 JsNumber(modified),
                 JsString(databaseName),
                 JsString(uniqueDatabaseName)) =>
          DxDatabaseDescribe(projectId,
                             id,
                             name,
                             folder,
                             created.toLong,
                             modified.toLong,
                             None,
                             None,
                             databaseName,
                             uniqueDatabaseName)
      }

    val details = descJs.fields.get("details")
    val props = descJs.fields.get("properties").map(DxObject.parseJsonProperties)
    desc.copy(details = details, properties = props)
  }
}
