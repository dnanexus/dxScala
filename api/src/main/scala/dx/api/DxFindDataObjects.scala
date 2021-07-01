package dx.api

import dx.util.Logger
import spray.json._

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
    val properties: Map[String, String] =
      fields.get("properties").map(DxObject.parseJsonProperties).getOrElse(Map.empty)
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
        DxAppDescribe(dxobj.id,
                      name,
                      created,
                      modified,
                      Some(properties),
                      details,
                      inputSpec,
                      outputSpec)
      case _: DxApplet =>
        DxAppletDescribe(dxProject.id,
                         dxobj.id,
                         name,
                         folder,
                         created,
                         modified,
                         Some(properties),
                         details,
                         inputSpec,
                         outputSpec,
                         tags = tags)
      case _: DxWorkflow =>
        val stages = fields.get("stages").map(DxWorkflowDescribe.parseStages)
        DxWorkflowDescribe(dxProject.id,
                           dxobj.id,
                           name,
                           folder,
                           created,
                           modified,
                           Some(properties),
                           details,
                           inputSpec,
                           outputSpec,
                           tags = tags,
                           stages = stages)
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
            dxProject.id,
            dxobj.id,
            name,
            folder,
            created,
            modified,
            size.get,
            state,
            archivalState,
            Some(properties),
            details,
            None
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
        val dxDesc = parseDescribe(desc, dxDataObject, dxProject)
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

  private def createScope(dxProject: DxProject,
                          folder: Option[String],
                          recurse: Boolean): JsValue = {
    val requiredFields =
      Map("project" -> JsString(dxProject.id), "recurse" -> JsBoolean(recurse))
    val folderFields = folder.map(path => Map("folder" -> JsString(path))).getOrElse(Map.empty)
    JsObject(requiredFields ++ folderFields)
  }

  // Submit a request for a limited number of objects
  private def submitRequest(
      scope: Option[JsValue],
      dxProject: Option[DxProject],
      cursor: JsValue,
      klass: Option[String],
      tagConstraints: Set[String],
      nameConstraints: Set[String],
      withInputOutputSpec: Boolean,
      idConstraints: Set[String],
      state: Option[DxState.DxState],
      defaultFields: Boolean,
      extraFields: Set[Field.Value]
  ): (Map[DxDataObject, DxObjectDescribe], JsValue) = {
    val requiredDescFields = Set(Field.Name,
                                 Field.Folder,
                                 Field.Size,
                                 Field.State,
                                 Field.ArchivalState,
                                 Field.Properties,
                                 Field.Created,
                                 Field.Modified) ++ extraFields
    val ioDescFields =
      if (withInputOutputSpec && klass.forall(Set("applet", "workflow").contains)) {
        Set(Field.InputSpec, Field.OutputSpec)
      } else {
        Set.empty
      }
    val requiredFields =
      Map(
          "visibility" -> JsString("either"),
          "describe" -> JsObject(
              "fields" -> DxObject.requestFields(requiredDescFields ++ ioDescFields),
              "defaultFields" -> JsBoolean(defaultFields)
          )
      )
    val projectField = dxProject.map(p => Map("project" -> JsString(p.id))).getOrElse(Map.empty)
    val scopeField = scope.map(s => Map("scope" -> s)).getOrElse(Map.empty)
    val limitField = limit.map(l => Map("limit" -> JsNumber(l))).getOrElse(Map.empty)
    val cursorField = cursor match {
      case JsNull => Map.empty
      case _      => Map("starting" -> cursor)
    }
    val classField = klass.map(k => Map("class" -> JsString(k))).getOrElse(Map.empty)
    val tagsField = tagConstraints.map(JsString(_)) match {
      case tags if tags.nonEmpty => Map("tagsArray" -> JsArray(tags.toVector))
      case _                     => Map.empty
    }
    val nameField = if (nameConstraints.isEmpty) {
      Map.empty
    } else if (nameConstraints.size == 1) {
      // Just one name, no need to use regular expressions
      Map("name" -> JsString(nameConstraints.head))
    } else {
      // Make a conjunction of all the legal names. For example:
      // ["Nice", "Foo", "Bar"] => ^Nice$|^Foo$|^Bar$
      val orRegexp = nameConstraints.map(x => s"^${x}$$").mkString("|")
      Map("name" -> JsObject("regexp" -> JsString(orRegexp)))
    }
    val idField = idConstraints match {
      case v if v.nonEmpty => Map("id" -> JsArray(v.map(JsString(_)).toVector))
      case _               => Map.empty
    }
    val stateField =
      state.map(s => Map("state" -> JsString(s.toString.toLowerCase))).getOrElse(Map.empty)
    val request = requiredFields ++ projectField ++ scopeField ++ cursorField ++ limitField ++ classField ++
      tagsField ++ nameField ++ idField ++ stateField
    if (scope.isEmpty) {
      logger.warning(
          """Calling findDataObjects without a project can cause result in longer response times 
            |and greater load on the API server""".stripMargin.replaceAll("\n", " ")
      )
      if (logger.isVerbose) {
        logger.traceLimited(JsObject(request).prettyPrint)
      }
    }
    val responseJs = dxApi.findDataObjects(request)
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
    val allowedClasses = Set("record", "file", "applet", "workflow")
    val invalidClasses = classRestriction.filterNot(allowedClasses.contains)
    if (invalidClasses.nonEmpty) {
      throw new Exception(
          s"invalid class limitation ${invalidClasses.mkString(",")}; must be one of {record, file, applet, workflow}"
      )
    }
    val scope: Option[JsValue] = dxProject.map(createScope(_, folder, recurse))
    val allResults: Map[DxDataObject, DxObjectDescribe] = Iterator
      .unfold[Map[DxDataObject, DxObjectDescribe], Option[JsValue]](Some(JsNull)) {
        case None => None
        case Some(cursor: JsValue) =>
          submitRequest(
              scope,
              dxProject,
              cursor,
              classRestriction,
              withTags,
              nameConstraints,
              withInputOutputSpec,
              idConstraints,
              state,
              defaultFields,
              extraFields
          ) match {
            case (results, _) if results.isEmpty => None
            case (results, JsNull)               => Some(results, None)
            case (results, next)                 => Some(results, Some(next))
          }
      }
      .flatten
      .toMap

    if (nameConstraints.isEmpty) {
      allResults
    } else {
      // Ensure the the data objects have names in the allowed set
      allResults.filter {
        case (_, desc) => nameConstraints.contains(desc.name)
      }
    }
  }
}
