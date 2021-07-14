package dx.api

import dx.util.{Enum, Logger}
import spray.json._

object DxVisibility extends Enum {
  type DxVisibility = Value
  val Visible, Hidden, Either = Value
}

case class DxFindDataObjectsConstraints(
    project: Option[DxProject] = None,
    folder: Option[String] = None,
    recurse: Boolean = false,
    objectClass: Option[String] = None,
    tags: Set[String] = Set.empty,
    properties: Option[DxConstraint] = None,
    names: Set[String] = Set.empty,
    nameRegexp: Option[String] = None,
    nameRegexpCaseInsensitive: Boolean = false,
    nameGlob: Option[String] = None,
    ids: Set[String] = Set.empty,
    state: Option[DxState.DxState] = None,
    createdBefore: Option[java.util.Date] = None,
    createdAfter: Option[java.util.Date] = None,
    modifiedBefore: Option[java.util.Date] = None,
    modifiedAfter: Option[java.util.Date] = None,
    visibility: DxVisibility.DxVisibility = DxVisibility.Either
) {
  def validate(): Unit = {
    val invalidClasses = objectClass.filterNot(DxFindDataObjectsConstraints.AllowedClasses.contains)
    if (invalidClasses.nonEmpty) {
      throw new Exception(
          s"""invalid class limitation ${invalidClasses.mkString(",")}; must be one of
             |${DxFindDataObjectsConstraints.AllowedClasses.mkString(",")}""".stripMargin
            .replaceAll("\n", " ")
      )
    }
  }

  lazy val toJson: Map[String, JsValue] = {
    val scopeField = project.map { proj =>
      val requiredFields = Map("project" -> JsString(proj.id), "recurse" -> JsBoolean(recurse))
      val folderFields = folder.map(path => Map("folder" -> JsString(path))).getOrElse(Map.empty)
      Map("scope" -> JsObject(requiredFields ++ folderFields))
    }
    val classField = objectClass.map(k => Map("class" -> JsString(k)))
    val tagsField = Option.when(tags.nonEmpty) {
      Map("tagsArray" -> JsArray(tags.map(JsString(_)).toVector))
    }
    val propertiesField = properties.map(constraint => Map("properties" -> constraint.toJson))
    val nameField = Vector(
        Option
          .when(names.nonEmpty) {
            if (names.size == 1) {
              // Just one name, no need to use regular expressions
              JsString(names.head)
            } else {
              // Make a conjunction of all the legal names. For example:
              // ["Nice", "Foo", "Bar"] => ^Nice$|^Foo$|^Bar$
              val orRegexp = names.map(x => s"^${x}$$").mkString("|")
              JsObject(
                  Vector(
                      Some("regexp" -> JsString(orRegexp)),
                      Option.when(nameRegexpCaseInsensitive)("flags" -> JsString("i"))
                  ).flatten.toMap
              )
            }
          },
        nameRegexp.map(regexp =>
          JsObject(
              Vector(
                  Some("regexp" -> JsString(regexp)),
                  Option.when(nameRegexpCaseInsensitive)("flags" -> JsString("i"))
              ).flatten.toMap
          )
        ),
        nameGlob.map(glob => JsObject("glob" -> JsString(glob)))
    ).flatten match {
      case Vector(arg) => Some(Map("name" -> arg))
      case Vector()    => None
      case _ =>
        throw new Exception("only one of 'names', 'nameRegexp', and 'nameGlob' may be defined")
    }
    val idField = Option.when(ids.nonEmpty) {
      Map("id" -> JsArray(ids.map(JsString(_)).toVector))
    }
    val stateField = state.map(s => Map("state" -> JsString(s.toString.toLowerCase)))
    val created = Vector(createdBefore.map(d => Map("before" -> JsNumber(d.getTime))),
                         createdAfter.map(d => Map("after" -> JsNumber(d.getTime)))).flatten
    val createdField =
      Option.when(created.nonEmpty)(Map("created" -> JsObject(created.flatten.toMap)))
    val modified = Vector(createdBefore.map(d => Map("before" -> JsNumber(d.getTime))),
                          createdAfter.map(d => Map("after" -> JsNumber(d.getTime)))).flatten
    val modifiedField =
      Option.when(modified.nonEmpty)(Map("modified" -> JsObject(modified.flatten.toMap)))
    val visibilityField = Some(Map("visibility" -> JsString(visibility.toString.toLowerCase())))
    Vector(scopeField,
           classField,
           tagsField,
           propertiesField,
           nameField,
           idField,
           stateField,
           createdField,
           modifiedField,
           visibilityField).flatten.flatten.toMap
  }
}

object DxFindDataObjectsConstraints {
  val AllowedClasses = Set("record", "file", "applet", "workflow")
}

case class DxFindDataObjects(dxApi: DxApi = DxApi.get,
                             limit: Option[Int] = None,
                             logger: Logger = Logger.get) {
  private def parseDescribe(jsv: JsValue,
                            dxobj: DxDataObject,
                            dxProject: DxProject): DxObjectDescribe = {
    val fields = jsv.asJsObject.fields
    // required fields
    val name: String = fields.get("name") match {
      case Some(JsString(name)) => name
      case None                 => throw new Exception("name field missing")
      case other                => throw new Exception(s"malformed name field ${other}")
    }
    val folder = fields.get("folder") match {
      case Some(JsString(folder)) => folder
      case None                   => throw new Exception("folder field missing")
      case Some(other)            => throw new Exception(s"malformed folder field ${other}")
    }
    val created: Long = fields.get("created") match {
      case Some(JsNumber(date)) => date.toLong
      case None                 => throw new Exception("'created' field is missing")
      case Some(other)          => throw new Exception(s"malformed created field ${other}")
    }
    val modified: Long = fields.get("modified") match {
      case Some(JsNumber(date)) => date.toLong
      case None                 => throw new Exception("'modified' field is missing")
      case Some(other)          => throw new Exception(s"malformed created field ${other}")
    }
    // possibly missing fields
    val size = fields.get("size").map {
      case JsNumber(size) => size.toLong
      case other          => throw new Exception(s"malformed size field ${other}")
    }
    val properties = fields.get("properties").map(DxObject.parseJsonProperties)
    val tags = fields.get("tags").flatMap {
      case JsArray(array) =>
        Some(array.map {
          case JsString(s) => s
          case other       => throw new Exception(s"invalid tag ${other}")
        }.toSet)
      case _ => None
    }
    val inputSpec: Option[Vector[IOParameter]] = fields.get("inputSpec") match {
      case Some(JsArray(iSpecVec)) =>
        Some(iSpecVec.map(iSpec => IOParameter.parseIoParam(dxApi, iSpec)))
      case None | Some(JsNull) => None
      case Some(other) =>
        throw new Exception(s"malformed inputSpec field ${other}")
    }
    val outputSpec: Option[Vector[IOParameter]] = fields.get("outputSpec") match {
      case Some(JsArray(oSpecVec)) =>
        Some(oSpecVec.map(oSpec => IOParameter.parseIoParam(dxApi, oSpec)))
      case None | Some(JsNull) => None
      case Some(other) =>
        throw new Exception(s"malformed output field ${other}")
    }
    val details: Option[JsValue] = fields.get("details")

    dxobj match {
      case _: DxApp =>
        DxAppDescribe(
            id = dxobj.id,
            name = name,
            created = created,
            modified = modified,
            tags = tags,
            properties = properties,
            details = details,
            inputSpec = inputSpec,
            outputSpec = outputSpec
        )
      case _: DxApplet =>
        DxAppletDescribe(
            project = dxProject.id,
            id = dxobj.id,
            name = name,
            folder = folder,
            created = created,
            modified = modified,
            tags = tags,
            properties = properties,
            details = details,
            inputSpec = inputSpec,
            outputSpec = outputSpec
        )
      case _: DxWorkflow =>
        val stages = fields.get("stages").map(DxWorkflowDescribe.parseStages)
        DxWorkflowDescribe(
            project = dxProject.id,
            id = dxobj.id,
            name = name,
            folder = folder,
            created = created,
            modified = modified,
            tags = tags,
            properties = properties,
            details = details,
            inputSpec = inputSpec,
            outputSpec = outputSpec,
            stages = stages
        )
      case _: DxFile =>
        val state = fields.get("state") match {
          case Some(JsString(x)) => DxState.withNameIgnoreCase(x)
          case None              => throw new Exception("'state' field is missing")
          case Some(other)       => throw new Exception(s"malformed state field ${other}")
        }
        val archivalState = fields.get("archivalState") match {
          case Some(JsString(x)) => DxArchivalState.withNameIgnoreCase(x)
          case None              => throw new Exception("'archivalState' field is missing")
          case Some(other)       => throw new Exception(s"malformed archivalState field ${other}")
        }
        DxFileDescribe(
            project = dxProject.id,
            id = dxobj.id,
            name = name,
            folder = folder,
            created = created,
            modified = modified,
            size = size.get,
            state = state,
            archivalState = archivalState,
            tags = tags,
            properties = properties,
            details = details,
            parts = None
        )
      case other =>
        throw new Exception(s"unsupported object ${other}")
    }
  }

  private def parseOneResult(jsv: JsValue): (DxDataObject, DxObjectDescribe) = {
    jsv.asJsObject.getFields("project", "id", "describe") match {
      case Seq(JsString(projectId), JsString(objectId), desc) =>
        val dxProject = dxApi.project(projectId)
        val dxDataObject = dxApi.dataObject(objectId, Some(dxProject))
        val dxDesc =
          try {
            parseDescribe(desc, dxDataObject, dxProject)
          } catch {
            case t: Throwable =>
              throw new Exception(s"Error parsing describe for ${dxDataObject}", t)
          }
        dxDataObject match {
          case dataObject: CachingDxObject[_] =>
            dataObject.cacheDescribe(dxDesc)
          case _ =>
            // TODO: make all data objects caching, and throw exception here
            ()
        }
        (dxDataObject, dxDesc)
      case _ =>
        throw new Exception(s"""|malformed result: expecting {project, id, describe} fields, got:
                                |${jsv.prettyPrint}
                                |""".stripMargin)
    }
  }

  // Submit a request for a limited number of objects
  private def submitRequest(
      request: Map[String, JsValue],
      cursor: JsValue
  ): (Map[DxDataObject, DxObjectDescribe], JsValue) = {
    val cursorField = cursor match {
      case JsNull => Map.empty
      case _      => Map("starting" -> cursor)
    }
    val responseJs = dxApi.findDataObjects(request ++ cursorField)
    val next: JsValue = responseJs.fields.get("next") match {
      case None | Some(JsNull) => JsNull
      case Some(obj: JsObject) => obj
      case Some(other)         => throw new Exception(s"malformed ${other.prettyPrint}")
    }
    val results: Vector[(DxDataObject, DxObjectDescribe)] =
      responseJs.fields.get("results") match {
        case Some(JsArray(results)) => results.map(parseOneResult)
        case None                   => throw new Exception(s"missing results field ${responseJs}")
        case Some(other)            => throw new Exception(s"malformed results field ${other.prettyPrint}")
      }
    (results.toMap, next)
  }

  def query(constraints: DxFindDataObjectsConstraints,
            withInputOutputSpec: Boolean = false,
            describe: Boolean = true,
            defaultFields: Boolean = false,
            extraFields: Set[Field.Value] = Set.empty): Map[DxDataObject, DxObjectDescribe] = {
    constraints.validate()
    val ioDescFields =
      if (withInputOutputSpec && constraints.objectClass
            .forall(Set("applet", "workflow").contains)) {
        Set(Field.InputSpec, Field.OutputSpec)
      } else {
        Set.empty
      }
    val requiredFields = Map(
        "describe" -> JsObject(
            "fields" -> DxObject.requestFields(
                DxFindDataObjects.RequiredDescFields ++ ioDescFields ++ extraFields
            ),
            "defaultFields" -> JsBoolean(defaultFields)
        )
    )
    val limitField = limit.map(l => Map("limit" -> JsNumber(l))).getOrElse(Map.empty)
    val request = requiredFields ++ limitField ++ constraints.toJson
    if (constraints.project.isEmpty) {
      logger.warning(
          """Calling findDataObjects without a project can cause result in longer response times
            |and greater load on the API server""".stripMargin.replaceAll("\n", " ")
      )
      if (logger.isVerbose) {
        logger.traceLimited(JsObject(request).prettyPrint)
      }
    }
    Iterator
      .unfold[Map[DxDataObject, DxObjectDescribe], Option[JsValue]](Some(JsNull)) {
        case None => None
        case Some(cursor: JsValue) =>
          submitRequest(request, cursor) match {
            case (results, _) if results.isEmpty => None
            case (results, JsNull)               => Some(results, None)
            case (results, next)                 => Some(results, Some(next))
          }
      }
      .flatten
      .toMap
  }

  /**
    * Search for data objects.
    * @param dxProject project to search in; None = search across all projects
    * @param folder folder to search in; None = root folder ("/")
    * @param recurse recurse into subfolders
    * @param classRestriction object classes to search
    * @param withTags objects must have these tags
    * @param nameConstraints object name has to be one of these strings
    * @param withInputOutputSpec should the IO spec be described?
    * @param idConstraints object must have one of these IDs
    * @param extraFields extra fields to describe
    * @return
    */
  def apply(dxProject: Option[DxProject],
            folder: Option[String],
            recurse: Boolean,
            classRestriction: Option[String] = None,
            withTags: Set[String] = Set.empty,
            nameConstraints: Set[String] = Set.empty,
            withInputOutputSpec: Boolean = false,
            idConstraints: Set[String] = Set.empty,
            state: Option[DxState.DxState] = None,
            defaultFields: Boolean = false,
            extraFields: Set[Field.Value] = Set.empty): Map[DxDataObject, DxObjectDescribe] = {
    val constraints = DxFindDataObjectsConstraints(
        project = dxProject,
        folder = folder,
        recurse = recurse,
        objectClass = classRestriction,
        tags = withTags,
        names = nameConstraints,
        ids = idConstraints,
        state = state
    )
    query(constraints = constraints,
          withInputOutputSpec = withInputOutputSpec,
          describe = true,
          defaultFields = defaultFields,
          extraFields = extraFields)
  }
}

object DxFindDataObjects {
  val RequiredDescFields = Set(Field.Name,
                               Field.Folder,
                               Field.Size,
                               Field.State,
                               Field.ArchivalState,
                               Field.Properties,
                               Field.Created,
                               Field.Modified)
}
