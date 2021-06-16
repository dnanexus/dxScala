package dx.api

import spray.json._

case class DxJobDescribe(id: String,
                         name: String,
                         project: DxProject,
                         created: Long,
                         modified: Long,
                         properties: Option[Map[String, String]],
                         details: Option[JsValue],
                         executableName: String,
                         parentJob: Option[DxJob],
                         analysis: Option[DxAnalysis],
                         executable: Option[DxExecutable],
                         output: Option[JsValue],
                         instanceType: Option[String],
                         folder: Option[String])
    extends DxObjectDescribe {
  override def containsAll(fields: Set[Field.Value]): Boolean = {
    fields.diff(DxJobDescribe.RequiredFields).forall {
      case Field.Properties   => properties.isDefined
      case Field.Details      => details.isDefined
      case Field.ParentJob    => parentJob.isDefined
      case Field.Analysis     => analysis.isDefined
      case Field.Executable   => executable.isDefined
      case Field.Output       => output.isDefined
      case Field.InstanceType => instanceType.isDefined
      case Field.Folder       => folder.isDefined
      case _                  => false
    }
  }
}

object DxJobDescribe {
  val RequiredFields =
    Set(Field.Id, Field.Name, Field.Project, Field.Created, Field.Modified, Field.ExecutableName)
  val DefaultFields: Set[Field.Value] = RequiredFields ++ Set(Field.ParentJob, Field.Analysis)
}

case class DxJob(id: String, project: Option[DxProject] = None)(dxApi: DxApi = DxApi.get)
    extends CachingDxObject[DxJobDescribe]
    with DxExecution {
  def describeNoCache(fields: Set[Field.Value] = Set.empty): DxJobDescribe = {
    val projSpec = DxObject.maybeSpecifyProject(project)
    val allFields = fields ++ DxJobDescribe.DefaultFields
    val descJs =
      dxApi.jobDescribe(id, projSpec + ("fields" -> DxObject.requestFields(allFields)))
    DxJob.parseDescribeJson(descJs, dxApi)
  }
}

object DxJob {
  def parseDescribeJson(descJs: JsObject, dxApi: DxApi = DxApi.get): DxJobDescribe = {
    val desc =
      descJs.getFields("id", "name", "project", "created", "modified", "executableName") match {
        case Seq(JsString(id),
                 JsString(name),
                 JsString(project),
                 JsNumber(created),
                 JsNumber(modified),
                 JsString(executableName)) =>
          DxJobDescribe(id,
                        name,
                        dxApi.project(project),
                        created.toLong,
                        modified.toLong,
                        None,
                        None,
                        executableName,
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
    val details = descJs.fields.get("details")
    val props = descJs.fields.get("properties").map(DxObject.parseJsonProperties)
    val parentJob: Option[DxJob] = descJs.fields.get("parentJob") match {
      case Some(JsString(x))   => Some(dxApi.job(x))
      case Some(JsNull) | None => None
      case Some(other)         => throw new Exception(s"should be a job ${other}")
    }
    val analysis = descJs.fields.get("analysis") match {
      case Some(JsString(x))   => Some(dxApi.analysis(x))
      case Some(JsNull) | None => None
      case Some(other)         => throw new Exception(s"should be an analysis ${other}")
    }
    val executable = descJs.fields.get("executable") match {
      case Some(JsString(id))  => Some(dxApi.executable(id, Some(desc.project)))
      case Some(JsNull) | None => None
      case Some(other)         => throw new Exception(s"should be an executable ID ${other}")
    }
    val output = descJs.fields.get("output")
    val instanceType = descJs.fields.get("instanceType") match {
      case Some(JsString(instanceType)) => Some(instanceType)
      case None                         => None
      case other                        => throw new Exception(s"should be an instance type ${other}")
    }
    val folder = descJs.fields.get("folder") match {
      case Some(JsString(folder)) => Some(folder)
      case None                   => None
      case other                  => throw new Exception(s"should be a folder ${other}")
    }
    desc.copy(details = details,
              properties = props,
              parentJob = parentJob,
              analysis = analysis,
              executable = executable,
              output = output,
              instanceType = instanceType,
              folder = folder)
  }
}
