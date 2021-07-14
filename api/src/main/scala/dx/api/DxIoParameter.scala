package dx.api

import spray.json._
import dx.util.{Enum, JsUtils, Logger}

object DxIOClass extends Enum {
  type DxIOClass = Value
  val Int, Float, String, Boolean, File, IntArray, FloatArray, StringArray, BooleanArray, FileArray,
      Hash, HashArray, Other = Value

  def fromString(s: String): DxIOClass.Value = {
    s.toLowerCase match {
      // primitives
      case "int"     => Int
      case "float"   => Float
      case "string"  => String
      case "boolean" => Boolean
      case "file"    => File

      // arrays of primitives
      case "array:int"     => IntArray
      case "array:float"   => FloatArray
      case "array:string"  => StringArray
      case "array:boolean" => BooleanArray
      case "array:file"    => FileArray

      // hash
      case "hash"       => Hash
      case "array:hash" => HashArray

      // we don't deal with anything else
      case _ => Other
    }
  }

  def isArray(cls: DxIOClass): Boolean = {
    Set(IntArray, FloatArray, StringArray, BooleanArray, HashArray).contains(cls)
  }
}

object DxIOSpec {
  val Name = "name"
  val Class = "class"
  val Optional = "optional"
  val Default = "default"
  val Choices = "choices"
  val Group = "group"
  val Help = "help"
  val Label = "label"
  val Patterns = "patterns"
  val Suggestions = "suggestions"
  val Type = "type"
}

// Types for the IO spec pattern section
sealed trait IOParameterPattern
case class IOParameterPatternArray(patterns: Vector[String]) extends IOParameterPattern
case class IOParameterPatternObject(name: Option[Vector[String]],
                                    klass: Option[String],
                                    tag: Option[Vector[String]])
    extends IOParameterPattern

// Types for the IO choices/suggestions/default
sealed trait IOParameterValue
final case class IOParameterValueString(value: String) extends IOParameterValue
final case class IOParameterValueNumber(value: BigDecimal) extends IOParameterValue
final case class IOParameterValueBoolean(value: Boolean) extends IOParameterValue
final case class IOParameterValueArray(array: Vector[IOParameterValue]) extends IOParameterValue
final case class IOParameterValueFile(value: Option[DxFile],
                                      name: Option[String] = None,
                                      project: Option[String] = None,
                                      path: Option[String] = None)
    extends IOParameterValue

object IOParameterValueFile {
  def forField(field: String,
               value: Option[DxFile],
               name: Option[String] = None,
               project: Option[String] = None,
               path: Option[String] = None): IOParameterValueFile = {
    field match {
      case DxIOSpec.Default if Seq(name, project, path).forall(_.isEmpty) =>
        IOParameterValueFile(value)
      case DxIOSpec.Default =>
        throw new Exception(
            s"file value may not have name, project, or path for field ${DxIOSpec.Default}"
        )
      case DxIOSpec.Choices if path.isEmpty =>
        IOParameterValueFile(value, name, project)
      case DxIOSpec.Choices =>
        throw new Exception(s"file value may not have path for field ${DxIOSpec.Choices}")
      case DxIOSpec.Suggestions =>
        IOParameterValueFile(value, name, project, path)
      case _ =>
        throw new Exception(s"unrecognized field ${field}")
    }
  }
}

// Representation of the IO spec
case class IOParameter(
    name: String,
    ioClass: DxIOClass.Value,
    optional: Boolean,
    group: Option[String] = None,
    help: Option[String] = None,
    label: Option[String] = None,
    patterns: Option[IOParameterPattern] = None,
    choices: Option[Vector[IOParameterValue]] = None,
    suggestions: Option[Vector[IOParameterValue]] = None,
    dxType: Option[DxConstraint] = None,
    default: Option[IOParameterValue] = None
)

object IOParameter {
  def parseIoParam(dxApi: DxApi, jsv: JsValue, logger: Logger = Logger.get): IOParameter = {
    val ioParam = jsv.asJsObject.getFields(DxIOSpec.Name, DxIOSpec.Class) match {
      case Seq(JsString(name), JsString(klass)) =>
        val ioClass = DxIOClass.fromString(klass)
        IOParameter(name, ioClass, optional = false)
      case other =>
        throw new Exception(s"Malformed io spec ${other}")
    }
    val optFlag = jsv.asJsObject.fields.get(DxIOSpec.Optional) match {
      case Some(JsBoolean(b)) => b
      case None               => false
    }
    val group = jsv.asJsObject.fields.get(DxIOSpec.Group) match {
      case Some(JsString(s)) => Some(s)
      case _                 => None
    }
    val help = jsv.asJsObject.fields.get(DxIOSpec.Help) match {
      case Some(JsString(s)) => Some(s)
      case _                 => None
    }
    val label = jsv.asJsObject.fields.get(DxIOSpec.Label) match {
      case Some(JsString(s)) => Some(s)
      case _                 => None
    }
    val patterns = jsv.asJsObject.fields.get(DxIOSpec.Patterns) match {
      case Some(JsArray(a)) =>
        Some(IOParameterPatternArray(a.flatMap {
          case JsString(s) => Some(s)
          case _           => None
        }))
      case Some(JsObject(obj)) =>
        val name = obj.get("name") match {
          case Some(JsArray(array)) =>
            Some(array.flatMap {
              case JsString(s) => Some(s)
              case _           => None
            })
          case _ => None
        }
        val tag = obj.get("tag") match {
          case Some(JsArray(array)) =>
            Some(array.flatMap {
              case JsString(s) => Some(s)
              case _           => None
            })
          case _ =>
            None
        }
        val klass = obj.get("class") match {
          case Some(JsString(s)) => Some(s)
          case _                 => None
        }
        Some(IOParameterPatternObject(name, klass, tag))
      case _ => None
    }

    def parseValue(key: String, value: JsValue): IOParameterValue = {
      (ioParam.ioClass, value) match {
        case (cls, JsArray(array)) if key == DxIOSpec.Default && DxIOClass.isArray(cls) =>
          IOParameterValueArray(array.map(value => parseValue(key, value)))
        case (DxIOClass.File | DxIOClass.FileArray, link @ JsObject(fields))
            if fields.contains(DxUtils.DxLinkKey) =>
          IOParameterValueFile.forField(key, Some(DxFile.fromJson(dxApi, link)))
        case (DxIOClass.File | DxIOClass.FileArray, JsObject(fields)) if key != DxIOSpec.Default =>
          val name = JsUtils.getOptionalString(fields, "name")
          val project = JsUtils.getOptionalString(fields, "project").flatMap { proj =>
            try {
              Some(dxApi.resolveProject(proj))
            } catch {
              case t: Throwable =>
                logger.error(s"Error resolving project ${proj}", exception = Some(t))
                None
            }
          }
          val path = Option
            .when(key == DxIOSpec.Suggestions)(JsUtils.getOptionalString(fields, "path"))
            .flatten
          val dxFile = fields.get("value") match {
            case Some(JsObject(fields))
                if fields.contains(DxUtils.DxLinkKey) && project.isDefined =>
              val link = fields(DxUtils.DxLinkKey) match {
                case fileId: JsString =>
                  JsObject("id" -> fileId, "project" -> JsString(project.get.id))
                case JsObject(fields) if fields.contains("id") =>
                  JsObject(fields + ("project" -> JsString(project.get.id)))
                case _ =>
                  throw new Exception(s"invalid DNAnexus link ${fields}")
              }
              Some(DxFile.fromJson(dxApi, JsObject(DxUtils.DxLinkKey -> link)))
            case Some(obj @ JsObject(fields)) if fields.contains(DxUtils.DxLinkKey) =>
              Some(DxFile.fromJson(dxApi, obj))
            case None if key == DxIOSpec.Suggestions && project.isDefined && path.isDefined =>
              try {
                Some(dxApi.resolveDataObject(path.get, project) match {
                  case file: DxFile => file
                  case other        => throw new Exception(s"expected object of type file, not ${other}")
                })
              } catch {
                case t: Throwable =>
                  logger.error(s"Error resolving ${project.get.id}:${path.get}",
                               exception = Some(t))
                  None
              }
            case None if project.isDefined && name.isDefined =>
              try {
                Some(dxApi.resolveDataObject(s"/${name.get}", project) match {
                  case file: DxFile => file
                  case other        => throw new Exception(s"expected object of type file, not ${other}")
                })
              } catch {
                case t: Throwable =>
                  logger.error(s"Error resolving ${project.get.id}:/${name.get}",
                               exception = Some(t))
                  None
              }
            case other =>
              throw new Exception(
                  s"choice value for parameter of type ${ioParam.ioClass} must be a DNAnexus link, not ${other}"
              )
          }
          IOParameterValueFile.forField(key, dxFile, name, project.map(_.id), path)
        case (DxIOClass.File | DxIOClass.FileArray, JsString(s)) if key != DxIOSpec.Default =>
          val dxFile =
            try {
              Some(dxApi.resolveFile(s))
            } catch {
              case t: Throwable =>
                logger.error(s"Error resolving file ${s}", exception = Some(t))
                None
            }
          IOParameterValueFile.forField(key, dxFile)
        case (DxIOClass.String | DxIOClass.StringArray, JsString(s)) =>
          IOParameterValueString(s)
        case (DxIOClass.Int | DxIOClass.IntArray, JsNumber(n)) if n.isValidLong =>
          IOParameterValueNumber(n)
        case (DxIOClass.Float | DxIOClass.FloatArray, JsNumber(n)) =>
          IOParameterValueNumber(n)
        case (DxIOClass.Boolean | DxIOClass.BooleanArray, JsBoolean(b)) =>
          IOParameterValueBoolean(b)
        case _ =>
          throw new Exception(s"Unexpected choice ${jsv} of type ${ioParam.ioClass}")
      }
    }

    val choices = JsUtils.getOptionalValues(jsv, DxIOSpec.Choices).map { array =>
      array.map(item => parseValue(DxIOSpec.Choices, item))
    }
    val suggestions = JsUtils.getOptionalValues(jsv, DxIOSpec.Choices).map { array =>
      array.map(item => parseValue(DxIOSpec.Suggestions, item))
    }
    val default = JsUtils.getOptional(jsv, DxIOSpec.Default).flatMap { value =>
      try {
        Some(parseValue(DxIOSpec.Default, value))
      } catch {
        // Currently, some valid defaults won't parse, so we ignore them for now
        case _: Exception => None
      }
    }
    val dxType = jsv.asJsObject.fields.get(DxIOSpec.Type) match {
      case Some(v: JsValue) => Some(ioParamTypeFromJs(v))
      case _                => None
    }
    ioParam.copy(
        optional = optFlag,
        group = group,
        help = help,
        label = label,
        patterns = patterns,
        choices = choices,
        suggestions = suggestions,
        dxType = dxType,
        default = default
    )
  }

  def ioParamTypeFromJs(value: JsValue): DxConstraint = {
    value match {
      case JsString(s) => DxConstraintString(s)
      case JsObject(fields) =>
        if (fields.size != 1) {
          throw new Exception("Constraint hash must have exactly one '$and' or '$or' key")
        }
        fields.head match {
          case (DxConstraintOper.And.name, JsArray(array)) =>
            DxConstraintBool(DxConstraintOper.And, DxConstraintArray(array.map(ioParamTypeFromJs)))
          case (DxConstraintOper.Or.name, JsArray(array)) =>
            DxConstraintBool(DxConstraintOper.Or, DxConstraintArray(array.map(ioParamTypeFromJs)))
          case _ =>
            throw new Exception(
                "Constraint must have key '$and' or '$or' and an array value"
            )
        }
      case _ => throw new Exception(s"Invalid paramter type value ${value}")
    }
  }

  def parseIOSpec(dxApi: DxApi, specs: Vector[JsValue]): Vector[IOParameter] = {
    specs.map(ioSpec => parseIoParam(dxApi, ioSpec))
  }
}
