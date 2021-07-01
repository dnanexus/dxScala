package dx.api

import java.nio.file.{FileVisitOption, FileVisitResult, Files, Path, SimpleFileVisitor}
import java.nio.file.attribute.BasicFileAttributes
import java.{util => javautil}
import com.dnanexus.{DXAPI, DXEnvironment}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import dx.api.DxPath.DxPathComponents
import dx.AppInternalException
import dx.util.{FileUtils, Logger, SysUtils, TraceLevel}
import dx.util.CollectionUtils.IterableOnceExtensions
import spray.json._

object DxApi {
  val ResultsPerCallLimit: Int = 1000
  val MaxNumDownloadBytes: Long = 2 * 1024 * 1024 * 1024

  private var instance: Option[DxApi] = None

  def get: DxApi =
    instance.getOrElse({
      instance = Some(DxApi()())
      instance.get
    })

  def set(dxApi: DxApi): Option[DxApi] = {
    val curDxApi = instance
    instance = Some(dxApi)
    curDxApi
  }

  def createAndSet(logger: Logger = Logger.get): DxApi = {
    val dxApi = DxApi()(logger)
    set(dxApi)
    dxApi
  }
}

/**
  * Wrapper around DNAnexus Java API
  * @param limit maximal number of objects in a single API request
  */
case class DxApi(version: String = "1.0.0", dxEnv: DXEnvironment = DXEnvironment.create())(
    logger: Logger = Logger.get,
    val limit: Int = DxApi.ResultsPerCallLimit
) {
  require(limit > 0 && limit <= DxApi.ResultsPerCallLimit)
  val currentProjectId: Option[String] = Option(dxEnv.getProjectContext)
  lazy val currentProject: Option[DxProject] = currentProjectId.map(DxProject(_)(this))
  val currentWorkspaceId: Option[String] = Option(dxEnv.getWorkspace)
  lazy val currentWorkspace: Option[DxProject] = currentWorkspaceId.map(DxProject(_)(this))
  val currentJobId: Option[String] = Option(dxEnv.getJob)
  lazy val currentJob: Option[DxJob] = currentJobId.map(DxJob(_)(this))
  // Convert from spray-json to jackson JsonNode
  // Used to convert into the JSON datatype used by dxjava
  private lazy val objMapper: ObjectMapper = new ObjectMapper()
  private val DownloadRetryLimit = 3
  private val UploadRetryLimit = 3
  private val UploadWaitMillis = 1000
  private val projectAndPathRegexp = "(?:(.+):)?(.+)\\s*".r

  /**
    * Calls 'dx pwd' and returns a tuple of (projectName, folder).
    */
  def getWorkingDir: (String, String) = {
    SysUtils.execCommand("dx pwd") match {
      case (_, projectAndPathRegexp(projName, path), _) => (projName, path)
      case other =>
        throw new Exception(s"unexpected 'dx pwd' output ${other}")
    }
  }

  // We are expecting string like:
  //    record-FgG51b00xF63k86F13pqFv57
  //    file-FV5fqXj0ffPB9bKP986j5kVQ
  //
  def getObject(id: String, container: Option[DxProject] = None): DxObject = {
    val (objType, _) = DxUtils.parseObjectId(id)
    objType match {
      case "analysis"  => DxAnalysis(id, container)(this)
      case "app"       => DxApp(id)(this)
      case "applet"    => DxApplet(id, container)(this)
      case "container" => DxProject(id)(this)
      case "file"      => DxFile(id, container)(this)
      case "job"       => DxJob(id, container)(this)
      case "project"   => DxProject(id)(this)
      case "record"    => DxRecord(id, container)(this)
      case "workflow"  => DxWorkflow(id, container)(this)
      case _ =>
        throw new IllegalArgumentException(
            s"${id} does not belong to a know DNAnexus object class"
        )
    }
  }

  def dataObject(id: String, project: Option[DxProject] = None): DxDataObject = {
    getObject(id, project) match {
      case dataObj: DxDataObject => dataObj
      case _                     => throw new IllegalArgumentException(s"${id} isn't a data object")
    }
  }

  def isDataObjectId(id: String): Boolean = {
    try {
      dataObject(id)
      true
    } catch {
      case _: Throwable => false
    }
  }

  def executable(id: String, project: Option[DxProject] = None): DxExecutable = {
    getObject(id, project) match {
      case exe: DxExecutable => exe
      case _                 => throw new IllegalArgumentException(s"${id} isn't an executable")
    }
  }

  // Lookup cache for projects. This saves repeated searches for projects we already found.
  private var projectDict: Map[String, DxProject] = Map.empty

  def resolveProject(projName: String): DxProject = {
    if (projectDict.contains(projName)) {
      return projectDict(projName)
    }

    if (projName.startsWith("project-")) {
      // A project ID
      return project(projName)
    }

    // A project name, resolve it
    val responseJs = findProjects(
        Map(
            "name" -> JsString(projName),
            "level" -> JsString("VIEW"),
            "limit" -> JsNumber(2)
        )
    )
    val results = responseJs.fields.get("results") match {
      case Some(JsArray(x)) => x
      case _ =>
        throw new Exception(
            s"""Bad response from systemFindProject API call (${responseJs.prettyPrint}),
               |when resolving project ${projName}.""".stripMargin.replaceAll("\n", " ")
        )
    }
    if (results.length > 1) {
      throw new Exception(s"Found more than one project named ${projName}")
    }
    if (results.isEmpty) {
      throw new Exception(s"Project ${projName} not found")
    }
    val dxProject = results(0).asJsObject.fields.get("id") match {
      case Some(JsString(id)) => project(id)
      case _ =>
        throw new Exception(
            s"Bad response from SystemFindProject API call ${responseJs.prettyPrint}"
        )
    }
    projectDict ++= Map(projName -> dxProject, dxProject.id -> dxProject)
    dxProject
  }

  def analysis(id: String, project: Option[DxProject] = None): DxAnalysis = {
    getObject(id, project) match {
      case a: DxAnalysis => a
      case _             => throw new IllegalArgumentException(s"${id} isn't an analysis")
    }
  }

  def app(id: String): DxApp = {
    getObject(id) match {
      case a: DxApp => a
      case _        => throw new IllegalArgumentException(s"${id} isn't an app")
    }
  }

  def applet(id: String, project: Option[DxProject] = None): DxApplet = {
    getObject(id, project) match {
      case a: DxApplet => a
      case _           => throw new IllegalArgumentException(s"${id} isn't an applet")
    }
  }

  def file(id: String, project: Option[DxProject] = None): DxFile = {
    getObject(id, project) match {
      case a: DxFile => a
      case _         => throw new IllegalArgumentException(s"${id} isn't a file")
    }
  }

  def job(id: String, project: Option[DxProject] = None): DxJob = {
    getObject(id, project) match {
      case a: DxJob => a
      case _        => throw new IllegalArgumentException(s"${id} isn't a job")
    }
  }

  def project(id: String): DxProject = {
    getObject(id) match {
      case a: DxProject => a
      case _            => throw new IllegalArgumentException(s"${id} isn't a project")
    }
  }

  def record(id: String, project: Option[DxProject] = None): DxRecord = {
    getObject(id, project) match {
      case a: DxRecord => a
      case _           => throw new IllegalArgumentException(s"${id} isn't a record")
    }
  }

  def workflow(id: String, project: Option[DxProject] = None): DxWorkflow = {
    getObject(id, project) match {
      case a: DxWorkflow => a
      case _             => throw new IllegalArgumentException(s"${id} isn't a workflow")
    }
  }

  private def call(fn: (Any, Class[JsonNode], DXEnvironment) => JsonNode,
                   fields: Map[String, JsValue] = Map.empty): JsObject = {
    val request = objMapper.readTree(JsObject(fields).prettyPrint)
    val response = fn(request, classOf[JsonNode], dxEnv)
    response.toString.parseJson.asJsObject
  }

  private def callObject(fn: (String, Any, Class[JsonNode], DXEnvironment) => JsonNode,
                         objectId: String,
                         fields: Map[String, JsValue] = Map.empty): JsObject = {
    val request = objMapper.readTree(JsObject(fields).prettyPrint)
    val response = fn(objectId, request, classOf[JsonNode], dxEnv)
    response.toString.parseJson.asJsObject
  }

  def analysisDescribe(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.analysisDescribe[JsonNode], id, fields)
  }

  def analysisAddTags(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.analysisAddTags[JsonNode], id, fields)
    logger.ignore(result)
  }

  def analysisRemoveTags(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.analysisRemoveTags[JsonNode], id, fields)
    logger.ignore(result)
  }

  def analysisSetProperties(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.analysisSetProperties[JsonNode], id, fields)
    logger.ignore(result)
  }

  def appDescribe(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.appDescribe[JsonNode], id, fields)
  }

  def appRun(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.appRun[JsonNode], id, fields)
  }

  def appletDescribe(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.appletDescribe[JsonNode], id, fields)
  }

  def appletGet(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.appletGet[JsonNode], id, fields)
  }

  def appletAddTags(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.appletAddTags[JsonNode], id, fields)
    logger.ignore(result)
  }

  def appletRemoveTags(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.appletRemoveTags[JsonNode], id, fields)
    logger.ignore(result)
  }

  def appletSetProperties(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.appletSetProperties[JsonNode], id, fields)
    logger.ignore(result)
  }

  def appletNew(fields: Map[String, JsValue]): JsObject = {
    call(DXAPI.appletNew[JsonNode], fields)
  }

  def appletRename(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.appletRename[JsonNode], id, fields)
  }

  def appletRun(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.appletRun[JsonNode], id, fields)
  }

  def containerDescribe(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.containerDescribe[JsonNode], id, fields)
  }

  def containerListFolder(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.containerListFolder[JsonNode], id, fields)
  }

  def containerMove(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.containerMove[JsonNode], id, fields)
    logger.ignore(result)
  }

  def containerNewFolder(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.containerNewFolder[JsonNode], id, fields)
    logger.ignore(result)
  }

  def containerRemoveFolder(id: String, fields: Map[String, JsValue]): Boolean = {
    val result = callObject(DXAPI.containerRemoveFolder[JsonNode], id, fields)
    result.fields.get("completed") match {
      case Some(JsBoolean(completed)) => completed
      case None                       => true
      case other =>
        throw new Exception(s"Invalid 'completed' value ${other}")
    }
  }

  def containerRemoveObjects(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.containerRemoveObjects[JsonNode], id, fields)
    logger.ignore(result)
  }

  def fileAddTags(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.fileAddTags[JsonNode], id, fields)
    logger.ignore(result)
  }

  def fileRemoveTags(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.fileRemoveTags[JsonNode], id, fields)
    logger.ignore(result)
  }

  def fileSetProperties(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.fileSetProperties[JsonNode], id, fields)
    logger.ignore(result)
  }

  def fileDescribe(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.fileDescribe[JsonNode], id, fields)
  }

  def jobDescribe(id: String, fields: Map[String, JsValue] = Map.empty): JsObject = {
    callObject(DXAPI.jobDescribe[JsonNode], id, fields)
  }

  def jobNew(fields: Map[String, JsValue]): JsObject = {
    call(DXAPI.jobNew[JsonNode], fields)
  }

  def runSubJob(entryPoint: String,
                instanceType: Option[String],
                inputs: JsValue,
                dependsOn: Vector[DxExecution],
                delayWorkspaceDestruction: Option[Boolean],
                name: Option[String] = None,
                details: Option[JsValue] = None): DxJob = {
    val requiredFields = Map(
        "function" -> JsString(entryPoint),
        "input" -> inputs
    )
    val instanceFields = instanceType match {
      case None => Map.empty
      case Some(iType) =>
        Map(
            "systemRequirements" -> JsObject(
                entryPoint -> JsObject("instanceType" -> JsString(iType))
            )
        )
    }
    val dependsFields =
      if (dependsOn.isEmpty) {
        Map.empty
      } else {
        val execIds = dependsOn.map { dxExec =>
          JsString(dxExec.id)
        }
        Map("dependsOn" -> JsArray(execIds))
      }
    val dwdFields = delayWorkspaceDestruction match {
      case Some(true) => Map("delayWorkspaceDestruction" -> JsTrue)
      case _          => Map.empty
    }
    val nameFields = name match {
      case Some(n) => Map("name" -> JsString(n))
      case None    => Map.empty
    }
    val detailsFields = details match {
      case Some(d) => Map("details" -> d)
      case None    => Map.empty
    }
    val request = requiredFields ++ instanceFields ++ dependsFields ++ dwdFields ++ nameFields ++ detailsFields
    logger.traceLimited(s"subjob request=${JsObject(request).prettyPrint}")
    val response = jobNew(request)
    val id: String = response.fields.get("id") match {
      case Some(JsString(x)) => x
      case _ =>
        throw new AppInternalException(s"Bad format returned from jobNew ${response.prettyPrint}")
    }
    job(id)
  }

  def orgDescribe(id: String, fields: Map[String, JsValue] = Map.empty): JsObject = {
    try {
      callObject(DXAPI.orgDescribe[JsonNode], id, fields)
    } catch {
      case cause: com.dnanexus.exceptions.PermissionDeniedException =>
        throw new dx.PermissionDeniedException(
            s"You do not have permission to describe org ${id}",
            cause
        )
    }
  }

  def projectClone(id: String, fields: Map[String, JsValue] = Map.empty): JsObject = {
    callObject(DXAPI.projectClone[JsonNode], id, fields)
  }

  def projectDescribe(id: String, fields: Map[String, JsValue] = Map.empty): JsObject = {
    callObject(DXAPI.projectDescribe[JsonNode], id, fields)
  }

  def projectListFolder(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.projectListFolder[JsonNode], id, fields)
  }

  def projectMove(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.projectMove[JsonNode], id, fields)
    logger.ignore(result)
  }

  def projectNewFolder(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.projectNewFolder[JsonNode], id, fields)
    logger.ignore(result)
  }

  def projectRemoveFolder(id: String, fields: Map[String, JsValue]): Boolean = {
    val result = callObject(DXAPI.projectRemoveFolder[JsonNode], id, fields)
    result.fields.get("completed") match {
      case Some(JsBoolean(completed)) => completed
      case None                       => true
      case other =>
        throw new Exception(s"Invalid 'completed' value ${other}")
    }
  }

  def projectRemoveObjects(id: String, fields: Map[String, JsValue]): Unit = {
    val result = callObject(DXAPI.projectRemoveObjects[JsonNode], id, fields)
    logger.ignore(result)
  }

  def recordDescribe(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.recordDescribe[JsonNode], id, fields)
  }

  def userDescribe(id: String, fields: Map[String, JsValue] = Map.empty): JsObject = {
    try {
      callObject(DXAPI.userDescribe[JsonNode], id, fields)
    } catch {
      case cause: com.dnanexus.exceptions.PermissionDeniedException =>
        throw new dx.PermissionDeniedException(
            s"You do not have permission to describe user ${id}",
            cause
        )
    }
  }

  def workflowClose(id: String): Unit = {
    callObject(DXAPI.workflowClose[JsonNode], id)
  }

  def workflowDescribe(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.workflowDescribe[JsonNode], id, fields)
  }

  def workflowNew(fields: Map[String, JsValue]): JsObject = {
    call(DXAPI.workflowNew[JsonNode], fields)
  }

  def workflowRename(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.workflowRename[JsonNode], id, fields)
  }

  def workflowRun(id: String, fields: Map[String, JsValue]): JsObject = {
    callObject(DXAPI.workflowRun[JsonNode], id, fields)
  }

  // system calls

  def whoami(): String = {
    call(DXAPI.systemWhoami[JsonNode]) match {
      case JsObject(fields) =>
        fields.get("id") match {
          case Some(JsString(id)) => id
          case other =>
            throw new Exception(s"unexpected whoami result ${other}")
        }
      case other =>
        throw new Exception(s"unexpected whoami result ${other}")
    }
  }

  lazy val isLoggedIn: Boolean = {
    try {
      whoami() != null
    } catch {
      case _: Throwable => false
    }
  }

  def findApps(fields: Map[String, JsValue]): JsObject = {
    call(DXAPI.systemFindApps[JsonNode], fields)
  }

  def resolveApp(name: String): DxApp = {
    findApps(Map("name" -> JsString(name))) match {
      case JsObject(fields) if fields.contains("results") =>
        fields("results") match {
          case JsArray(results) if results.size == 1 =>
            val result = results(0).asJsObject.fields
            val JsString(id) = result("id")
            val app = DxApp(id)(this)
            result.get("describe").foreach { descJs =>
              val desc = DxApp.parseDescribeJson(descJs.asJsObject, this)
              app.cacheDescribe(desc)
            }
            app
          case other =>
            throw new Exception(s"expected 1 result, got ${other}")
        }
      case other =>
        throw new Exception(s"invalid findApps response ${other}")
    }
  }

  def findDataObjects(fields: Map[String, JsValue]): JsObject = {
    call(DXAPI.systemFindDataObjects[JsonNode], fields)
  }

  def resolveDataObjects(fields: Map[String, JsValue]): JsObject = {
    call(DXAPI.systemResolveDataObjects[JsonNode], fields)
  }

  private def triagePath(components: DxPathComponents): Either[DxDataObject, DxPathComponents] = {
    if (isDataObjectId(components.name)) {
      val dxDataObj = dataObject(components.name)
      val dxDataObjWithProj = components.projName match {
        case None => dxDataObj
        case Some(pid) =>
          val dxProj = resolveProject(pid)
          dataObject(dxDataObj.id, Some(dxProj))
      }
      Left(dxDataObjWithProj)
    } else {
      Right(components)
    }
  }

  // Create a request from a path like:
  //   "dx://dxWDL_playground:/test_data/fileB",
  private def createResolutionRequest(components: DxPathComponents): JsValue = {
    val reqFields: Map[String, JsValue] = Map("name" -> JsString(components.name))
    val folderField: Map[String, JsValue] = components.folder match {
      case None    => Map.empty
      case Some(x) => Map("folder" -> JsString(x))
    }
    val projectField: Map[String, JsValue] = components.projName match {
      case None => Map.empty
      case Some(x) =>
        val dxProj = resolveProject(x)
        Map("project" -> JsString(dxProj.id))
    }
    JsObject(reqFields ++ folderField ++ projectField)
  }

  private def submitResolutionRequest(dxPaths: Vector[DxPathComponents],
                                      dxProject: DxProject): Map[String, DxDataObject] = {
    val objectReqs: Vector[JsValue] = dxPaths.map(createResolutionRequest)
    val request = Map("objects" -> JsArray(objectReqs), "project" -> JsString(dxProject.id))
    val responseJs = resolveDataObjects(request)
    val resultsPerObj: Vector[JsValue] = responseJs.fields.get("results") match {
      case Some(JsArray(x)) => x
      case other            => throw new Exception(s"API call returned invalid data ${other}")
    }
    resultsPerObj.zipWithIndex.map {
      case (descJs: JsValue, i) =>
        val path = dxPaths(i).sourcePath
        val o = descJs match {
          case JsArray(x) if x.isEmpty =>
            throw new Exception(
                s"Path ${path} not found req=${objectReqs(i)}, i=${i}, project=${dxProject.id}"
            )
          case JsArray(x) if x.length == 1 => x(0)
          case JsArray(_) =>
            throw new Exception(s"Found more than one dx object in path ${path}")
          case obj: JsObject => obj
          case other         => throw new Exception(s"malformed json ${other}")
        }
        val fields = o.asJsObject.fields
        val dxid = fields.get("id") match {
          case Some(JsString(x)) => x
          case _                 => throw new Exception("no id returned")
        }

        // could be a container, not a project
        val dxContainer: Option[DxProject] = fields.get("project") match {
          case Some(JsString(x)) => Some(project(x))
          case _                 => None
        }

        // safe conversion to a dx-object
        path -> dataObject(dxid, dxContainer)
    }.toMap
  }

  def resolveDataObject(dxPath: String,
                        dxProject: Option[DxProject] = None,
                        dxPathComponents: Option[DxPathComponents] = None): DxDataObject = {
    val components = dxPathComponents.getOrElse(DxPath.parse(dxPath))
    // search in the dxProject or the project specified in dxPath, if any,
    // otherwise search in the current workspace and project
    lazy val searchContainers = dxProject
      .orElse(components.projName.map(resolveProject))
      .map(Vector(_))
      .getOrElse(
          Vector(
              currentWorkspace,
              currentProject
          ).flatten
      )

    // peel off objects that have already been resolved
    triagePath(components) match {
      case Left(alreadyResolved) => alreadyResolved
      case Right(_) if searchContainers.isEmpty =>
        throw new Exception(
            s"${dxPath} was not already resolved, and there are no containers to search"
        )
      case Right(dxPathsToResolve) =>
        searchContainers.iterator
          .map { proj =>
            (submitResolutionRequest(Vector(dxPathsToResolve), proj).values.toVector, proj)
          }
          .collectFirst {
            case (Vector(result), _) => result
            case (result, proj) if result.size > 1 =>
              throw new Exception(
                  s"Found more than one dx:object for path ${dxPath} in project=${proj}"
              )
          }
          .getOrElse(
              throw new Exception(s"Could not find ${dxPath} in any of ${searchContainers}")
          )
    }
  }

  // Describe the names of all the data objects in one batch. This is much more efficient
  // than submitting object describes one-by-one.
  def resolveDataObjectBulk(dxPaths: Seq[String],
                            dxProject: DxProject): Map[String, DxDataObject] = {
    if (dxPaths.isEmpty) {
      // avoid an unnessary API call; this is important for unit tests
      // that do not have a network connection.
      return Map.empty
    }
    // split between files that have already been resolved (we have their file-id), and
    // those that require lookup.
    val (alreadyResolved, dxPathsToResolve) =
      dxPaths.toSet.foldLeft((Map.empty[String, DxDataObject], Vector.empty[DxPathComponents])) {
        case ((alreadyResolved, rest), p) =>
          triagePath(DxPath.parse(p)) match {
            case Left(dxObjWithProj) =>
              (alreadyResolved + (p -> dxObjWithProj), rest)
            case Right(components) =>
              (alreadyResolved, rest :+ components)
          }
      }
    if (dxPathsToResolve.isEmpty) {
      alreadyResolved
    } else {
      // Limit on number of objects in one API request
      val slices = dxPathsToResolve.grouped(limit).toList
      // iterate on the ranges
      val resolved = slices.foldLeft(Map.empty[String, DxDataObject]) {
        case (accu, pathsRange) =>
          accu ++ submitResolutionRequest(pathsRange, dxProject)
      }
      alreadyResolved ++ resolved
    }
  }

  /**
    * Describe the names of all the files in one batch. This is much more efficient
    * than submitting file describes one-by-one.
    * Note: this function does *not* necessarily return the files in the same order
    * they are passed.
    * @param files files to describe
    * @param extraFields extra fields to describe
    * @param searchWorkspaceFirst whether to search for files by ID in the current
    *                             DX_WORKSPACE before searching by project
    * @param validate check that exaclty one result is returned for each file
    * @return
    */
  def describeFilesBulk(
      files: Vector[DxFile],
      extraFields: Set[Field.Value] = Set.empty,
      searchWorkspaceFirst: Boolean = false,
      validate: Boolean = false
  ): Vector[DxFile] = {
    if (files.isEmpty) {
      // avoid an unnessary API call; this is important for unit tests
      // that do not have a network connection.
      return Vector.empty
    }

    val dxFindDataObjects = DxFindDataObjects(this, None)

    // Describe a large number of platform files in bulk. Chunk files
    // to limit the number of objects in one API request. DxFindDataObjects
    // caches the desc on the DxFile object, so we only need to return the DxFile.
    def submitRequest(ids: Set[String], project: Option[DxProject]): Vector[DxFile] = {
      ids
        .grouped(limit)
        .flatMap { chunk =>
          dxFindDataObjects
            .apply(
                dxProject = project,
                folder = None,
                recurse = true,
                classRestriction = Some("file"),
                withInputOutputSpec = true,
                idConstraints = chunk,
                extraFields = extraFields
            )
            .keys
        }
        .map {
          case file: DxFile => file
          case other        => throw new Exception(s"non-file result ${other}")
        }
        .toVector
    }

    lazy val filesById: Map[String, DxFile] = files.map(f => f.id -> f).toMap

    val (workspaceResults, remaining) = if (searchWorkspaceFirst && currentWorkspace.isDefined) {
      val workspaceResults = submitRequest(filesById.keySet, currentWorkspace)
      val workspaceFileIds = workspaceResults.map(_.id).toSet
      val remaining = filesById.collect {
        case (id, file) if !workspaceFileIds.contains(id) => file
      }
      (workspaceResults, remaining)
    } else {
      (Vector.empty, files)
    }

    val allResults = workspaceResults ++ remaining.groupBy(_.project).flatMap {
      case (proj, files) => submitRequest(files.map(_.id).toSet, proj)
    }

    if (validate) {
      val allResultsById = allResults.groupBy(_.id)
      val multiple = allResultsById.filter(_._2.size > 1)
      if (multiple.nonEmpty) {
        throw new Exception(
            s"One or more file IDs did not have a project specified and returned multiple search results: ${multiple}"
        )
      }
      val missing = filesById.keySet.diff(allResultsById.keySet)
      if (missing.nonEmpty) {
        throw new Exception(s"One or more file id(s) were not found: ${missing.mkString(",")}")
      }
    }

    allResults
  }

  def resolveFile(uri: String): DxFile = {
    resolveDataObject(uri) match {
      case dxfile: DxFile => dxfile
      case other =>
        throw new Exception(s"Found dx:object of the wrong type ${other}")
    }
  }

  def resolveRecord(uri: String): DxRecord = {
    resolveDataObject(uri) match {
      case dxrec: DxRecord =>
        dxrec
      case other =>
        throw new Exception(s"Found dx:object of the wrong type ${other}")
    }
  }

  def findExecutions(fields: Map[String, JsValue]): JsObject = {
    call(DXAPI.systemFindExecutions[JsonNode], fields)
  }

  def findProjects(fields: Map[String, JsValue]): JsObject = {
    call(DXAPI.systemFindProjects[JsonNode], fields)
  }

  // copy asset to local project, if it isn't already here.
  def cloneAsset(assetName: String,
                 assetRecord: DxRecord,
                 sourceProject: DxProject,
                 destProject: DxProject,
                 destFolder: String = "/"): Unit = {
    if (sourceProject.id == destProject.id) {
      logger.trace(
          s"""The source and destination projects are the same (${sourceProject.id}), 
             |no need to clone asset ${assetName}""".stripMargin.replaceAll("\n", " ")
      )
    } else {
      logger.trace(s"Cloning asset ${assetName} from ${sourceProject.id} to ${destProject.id}")
      val request = Map("objects" -> JsArray(JsString(assetRecord.id)),
                        "project" -> JsString(destProject.id),
                        "destination" -> JsString(destFolder))
      val responseJs = projectClone(sourceProject.id, request)
      val existingIds = responseJs.fields.get("exists") match {
        case Some(JsArray(x)) =>
          x.flatMap {
            case JsString(id) if DxUtils.isRecordId(id) =>
              Some(id)
            case JsString(id) =>
              logger.trace(s"ignoring non-record id ${id}", minLevel = TraceLevel.VVerbose)
              None
            case other =>
              throw new Exception(s"expected 'exists' field to be a string, not ${other}")
          }
        case None =>
          throw new Exception("API call did not return an exists field")
        case _ =>
          throw new Exception(s"API call returned invalid exists field")
      }
      existingIds match {
        case Vector() =>
          logger.trace(
              s"Created ${assetRecord.id} in ${destProject.id} pointing to asset ${assetName}"
          )
        case Vector(_) =>
          logger.trace(
              s"The destination project ${destProject.id} already has a record pointing to asset ${assetName}"
          )
        case _ =>
          throw new Exception(
              s"clone returned too many existing records ${existingIds} in destination project ${destProject.id}"
          )
      }
    }
  }

  /**
    * Downloads a file from the platform to a path on the local disk.
    * Calls `dx download` as a subprocess.
    * @param path the target path
    * @param dxfile the dx file to download
    * @param overwrite whether to overwrite an existing file - if false, an
    *                  exception is thrown if the target path already exists
    */
  def downloadFile(path: Path, dxfile: DxFile, overwrite: Boolean = false): Unit = {
    def downloadOneFile(path: Path, dxfile: DxFile): Boolean = {
      val fid = dxfile.id
      val fileObj = path.toFile
      val alreadyExists = fileObj.exists()

      try {
        // Use dx download. Quote the path, because it may contains spaces.
        val dxDownloadCmd =
          s"""dx download ${fid} -o "${path.toString}" --no-progress ${if (overwrite) "-f" else ""}"""
        logger.traceLimited(s"--  ${dxDownloadCmd}")
        val (_, stdout, stderr) = SysUtils.execCommand(dxDownloadCmd)
        if (stdout.nonEmpty) {
          logger.warning(s"unexpected output: ${stdout}")
          false
        } else if (stderr.nonEmpty) {
          logger.warning(s"unexpected error: ${stderr}")
          false
        } else {
          true
        }
      } catch {
        case e: Throwable =>
          logger.traceLimited(s"error downloading file ${dxfile}", exception = Some(e))
          // the file may have been partially downloaded - delete it before we retry
          if ((overwrite || !alreadyExists) && fileObj.exists()) {
            fileObj.delete()
          }
          false
      }
    }

    val dir = path.getParent
    if (dir != null && !Files.exists(dir)) {
      Files.createDirectories(dir)
    }
    // we rely on the fact that exists() exits as soon as it encounters `true`
    val success = Iterator
      .range(0, DownloadRetryLimit)
      .exists { counter =>
        if (counter > 0) {
          Thread.sleep(1000)
        }
        logger.traceLimited(s"downloading file ${path.toString} (try=${counter})")
        downloadOneFile(path, dxfile)
      }
    if (!success) {
      throw new Exception(s"Failure to download file ${path}")
    }
  }

  private def silentFileDelete(p: Path): Unit = {
    try {
      Files.delete(p)
    } catch {
      case _: Throwable => ()
    }
  }

  // Read the contents of a platform file into a byte array
  def downloadBytes(dxFile: DxFile): Array[Byte] = {
    // create a temporary file, and write the contents into it.
    val tempFile: Path = Files.createTempFile(s"${dxFile.id}", ".tmp")
    silentFileDelete(tempFile)
    val content =
      try {
        downloadFile(tempFile, dxFile)
        FileUtils.readFileBytes(tempFile)
      } finally {
        silentFileDelete(tempFile)
      }
    content
  }

  /**
    * Uploads a local file to the platform, and returns a DxFile.
    * Calls `dx upload` in a subprocess.
    * TODO: once https://jira.internal.dnanexus.com/browse/DEVEX-1939 is
    *  implemented, load the returned JSON and cache it for describe
    */
  def uploadFile(path: Path,
                 destination: Option[String] = None,
                 wait: Boolean = false,
                 tags: Set[String] = Set.empty,
                 properties: Map[String, String] = Map.empty): DxFile = {
    if (!Files.exists(path)) {
      throw new AppInternalException(s"Output file ${path.toString} is missing")
    }

    val (destProj, _) = destination match {
      case Some(projectAndPathRegexp(proj, path)) if path.endsWith("/") =>
        (Option(proj), path)
      case Some(projectAndPathRegexp(proj, path)) =>
        (Option(proj), FileUtils.getPath(path).getParent.toString)
      case None => (None, getWorkingDir._2)
      case _ =>
        throw new Exception(s"invalid destination ${destination}")
    }

    def uploadOneFile(path: Path): Option[String] = {
      try {
        // shell out to dx upload. We need to quote the path, because it may contain spaces
        val destOpt = destination.map(d => s""" --destination "${d}" -p""").getOrElse("")
        val waitOpt = if (wait) " --wait" else ""
        val tagsOpt = tags.map(tag => s" --tag ${tag}").mkString("")
        val propertiesOpt = properties
          .map {
            case (key, value) => s" --property ${key}=${value}"
          }
          .mkString("")
        val dxUploadCmd =
          s"""dx upload "${path.toString}" --brief${destOpt}${waitOpt}${tagsOpt}${propertiesOpt}"""
        logger.traceLimited(s"CMD: ${dxUploadCmd}")
        SysUtils.execCommand(dxUploadCmd) match {
          case (_, stdout, _) if stdout.trim.startsWith("file-") =>
            Some(stdout.trim())
          case (_, stdout, stderr) =>
            logger.traceLimited(s"""unexpected response:
                                   |stdout: ${stdout}
                                   |stderr: ${stderr}""".stripMargin)
            None
        }
      } catch {
        case e: Throwable =>
          logger.traceLimited(s"error uploading file ${path}", exception = Some(e))
          None
      }
    }

    Iterator
      .range(0, UploadRetryLimit)
      .collectFirstDefined { counter =>
        if (counter > 0) {
          Thread.sleep(UploadWaitMillis * scala.math.pow(2, counter).toLong)
        }
        logger.traceLimited(s"upload file ${path.toString} (try=${counter})")
        uploadOneFile(path) match {
          case Some(fid) => Some(file(fid, destProj.map(resolveProject)))
          case None      => None
        }
      }
      .getOrElse(throw new Exception(s"Failure to upload file ${path}"))
  }

  def uploadString(content: String,
                   destination: String,
                   wait: Boolean = false,
                   tags: Set[String] = Set.empty,
                   properties: Map[String, String] = Map.empty): DxFile = {
    // create a temporary file, and write the contents into it.
    val tempFile: Path = Files.createTempFile("upload", ".tmp")
    silentFileDelete(tempFile)
    val path = FileUtils.writeFileContent(tempFile, content)
    uploadFile(path, Some(destination), wait = wait, tags = tags, properties = properties)
  }

  private case class UploadFileVisitor(sourceDir: Path,
                                       destination: Option[String],
                                       waitOnUpload: Boolean,
                                       filter: Option[Path => Boolean])
      extends SimpleFileVisitor[Path] {
    private def ensureEndsWithSlash(path: String): String = {
      if (path.endsWith("/")) path else s"${path}/"
    }

    val (projectId: Option[String], folder: String) = destination
      .map { dest =>
        dest.split(":").toVector match {
          case Vector(projectId) if projectId.startsWith("project-") =>
            (Some(projectId), "/")
          case Vector(folder) =>
            (None, ensureEndsWithSlash(folder))
          case Vector(projectId, folder) =>
            (Some(projectId), ensureEndsWithSlash(folder))
          case _ =>
            throw new Exception(s"invalid destination ${dest}")
        }
      }
      .getOrElse((None, "/"))

    private var uploadedFiles = Map.empty[Path, DxFile]

    def result: (Option[String], String, Map[Path, DxFile]) = {
      (projectId, folder, uploadedFiles)
    }

    override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
      if (!attrs.isDirectory && filter.forall(f => f(file))) {
        val fileRelPath = sourceDir.relativize(file)
        val fileDestPath = s"${folder}${fileRelPath}"
        val fileDest = projectId.map(p => s"${p}:${fileDestPath}").getOrElse(fileDestPath)
        val dxFile = uploadFile(file, Some(fileDest), waitOnUpload)
        uploadedFiles += (file -> dxFile)
      }
      FileVisitResult.CONTINUE
    }
  }

  /**
    * Uploads all the files in a directory, and - if `recursive=true` - all
    * files in subfolders as well. Returns (projectId, folder, files), where
    * files is a Map from local path to DxFile.
    * @param path directory to upload
    * @param destination optional destination folder in the current project, or
    *                    "project-xxx:/path/to/folder"
    * @param recursive whether to upload all subdirectories
    * @param wait whether to wait for each upload to complete
    * @param filter optional filter function to determine which files to upload
    */
  def uploadDirectory(
      path: Path,
      destination: Option[String] = None,
      recursive: Boolean = true,
      wait: Boolean = false,
      filter: Option[Path => Boolean] = None
  ): (Option[String], String, Map[Path, DxFile]) = {
    val visitor = UploadFileVisitor(path, destination, wait, filter)
    val maxDepth = if (recursive) Integer.MAX_VALUE else 1
    Files.walkFileTree(path, javautil.EnumSet.noneOf(classOf[FileVisitOption]), maxDepth, visitor)
    visitor.result
  }
}
