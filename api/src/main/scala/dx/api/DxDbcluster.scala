package dx.api

import spray.json._

case class DxDbclusterDescribe(project: String,
                              id: String,
                              name: String,
                              folder: String,
                              created: Long,
                              modified: Long,
                              properties: Option[Map[String, String]],
                              details: Option[JsValue])
    extends DxObjectDescribe

case class DxDbcluster(id: String, project: Option[DxProject])(dxApi: DxApi = DxApi.get)
    extends DxDataObject {
  def describe(fields: Set[Field.Value] = Set.empty): DxDbclusterDescribe = {
    val projSpec = DxObject.maybeSpecifyProject(project)
    val defaultFields =
      Set(Field.Project, Field.Id, Field.Name, Field.Folder, Field.Created, Field.Modified)
    val allFields = fields ++ defaultFields
    val descJs =
      dxApi.dbclusterDescribe(id, projSpec + ("fields" -> DxObject.requestFields(allFields)))
    val desc =
      descJs.getFields("project", "id", "name", "folder", "created", "modified") match {
        case Seq(JsString(projectId),
                 JsString(id),
                 JsString(name),
                 JsString(folder),
                 JsNumber(created),
                 JsNumber(modified)) =>
          DxDbclusterDescribe(projectId, id, name, folder, created.toLong, modified.toLong, None, None)
      }

    val details = descJs.fields.get("details")
    val props = descJs.fields.get("properties").map(DxObject.parseJsonProperties)
    desc.copy(details = details, properties = props)
  }
}
