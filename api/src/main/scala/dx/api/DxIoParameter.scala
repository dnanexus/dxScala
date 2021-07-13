package dx.api

import spray.json._
import dx.util.{Enum, JsUtils}

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

// Types for the IO choices section
sealed trait IOParameterChoice
final case class IOParameterChoiceString(value: String) extends IOParameterChoice
final case class IOParameterChoiceNumber(value: BigDecimal) extends IOParameterChoice
final case class IOParameterChoiceBoolean(value: Boolean) extends IOParameterChoice
final case class IOParameterChoiceFile(value: DxFile, name: Option[String] = None)
    extends IOParameterChoice

// Types for the IO 'default' section
sealed trait IOParameterDefault
final case class IOParameterDefaultString(value: String) extends IOParameterDefault
final case class IOParameterDefaultNumber(value: BigDecimal) extends IOParameterDefault
final case class IOParameterDefaultBoolean(value: Boolean) extends IOParameterDefault
final case class IOParameterDefaultFile(value: DxFile) extends IOParameterDefault
final case class IOParameterDefaultArray(array: Vector[IOParameterDefault])
    extends IOParameterDefault

// Representation of the IO spec
case class IOParameter(
    name: String,
    ioClass: DxIOClass.Value,
    optional: Boolean,
    group: Option[String] = None,
    help: Option[String] = None,
    label: Option[String] = None,
    patterns: Option[IOParameterPattern] = None,
    choices: Option[Vector[IOParameterChoice]] = None,
    suggestions: Option[Vector[IOParameterChoice]] = None,
    dxType: Option[DxConstraint] = None,
    default: Option[IOParameterDefault] = None
)

object IOParameter {
  def parseIoParam(dxApi: DxApi, jsv: JsValue): IOParameter = {
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

    def parseChoices(key: String, pathAllowed: Boolean): Option[Vector[IOParameterChoice]] = {
      JsUtils.getOptionalValues(jsv, key).map { array =>
        array.map { item =>
          (ioParam.ioClass, item) match {
            case (DxIOClass.File | DxIOClass.FileArray, link @ JsObject(fields))
                if fields.contains(DxUtils.DxLinkKey) =>
              IOParameterChoiceFile(DxFile.fromJson(dxApi, link))
            case (DxIOClass.File | DxIOClass.FileArray, JsObject(fields)) =>
              val name = JsUtils.getOptionalString(fields, "name")
              val project = JsUtils.getOptionalString(fields, "project").map(dxApi.resolveProject)
              val path = Option.when(pathAllowed)(JsUtils.getOptionalString(fields, "path")).flatten
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
                  DxFile.fromJson(dxApi, JsObject(DxUtils.DxLinkKey -> link))
                case Some(obj @ JsObject(fields)) if fields.contains(DxUtils.DxLinkKey) =>
                  DxFile.fromJson(dxApi, obj)
                case None if project.isDefined && path.isDefined =>
                  dxApi.resolveDataObject(path.get, project) match {
                    case file: DxFile => file
                    case other        => throw new Exception(s"expected object of type file, not ${other}")
                  }
                case None if project.isDefined && name.isDefined =>
                  dxApi.resolveDataObject(s"/${name.get}", project) match {
                    case file: DxFile => file
                    case other        => throw new Exception(s"expected object of type file, not ${other}")
                  }
                case other =>
                  throw new Exception(
                      s"choice value for parameter of type ${ioParam.ioClass} must be a DNAnexus link, not ${other}"
                  )
              }
              IOParameterChoiceFile(dxFile, name)
            case (DxIOClass.File | DxIOClass.FileArray, JsString(s)) =>
              IOParameterChoiceFile(dxApi.resolveFile(s))
            case (DxIOClass.String | DxIOClass.StringArray, JsString(s)) =>
              IOParameterChoiceString(s)
            case (DxIOClass.Int | DxIOClass.IntArray, JsNumber(n)) if n.isValidLong =>
              IOParameterChoiceNumber(n)
            case (DxIOClass.Float | DxIOClass.FloatArray, JsNumber(n)) =>
              IOParameterChoiceNumber(n)
            case (DxIOClass.Boolean | DxIOClass.BooleanArray, JsBoolean(b)) =>
              IOParameterChoiceBoolean(b)
            case _ =>
              throw new Exception(s"Unexpected choice ${jsv} of type ${ioParam.ioClass}")
          }
        }
      }
    }

    val choices = parseChoices(DxIOSpec.Choices, pathAllowed = false)
    val suggestions = parseChoices(DxIOSpec.Suggestions, pathAllowed = true)
    val dxType = jsv.asJsObject.fields.get(DxIOSpec.Type) match {
      case Some(v: JsValue) => Some(ioParamTypeFromJs(v))
      case _                => None
    }
    val default = jsv.asJsObject.fields.get(DxIOSpec.Default) match {
      case Some(v: JsValue) =>
        try {
          Some(ioParamDefaultFromJs(dxApi, v))
        } catch {
          // Currently, some valid defaults won't parse, so we ignore them for now
          case _: Exception => None
        }
      case _ => None
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

  def ioParamDefaultFromJs(dxApi: DxApi, value: JsValue): IOParameterDefault = {
    value match {
      case JsString(s)       => IOParameterDefaultString(s)
      case JsNumber(n)       => IOParameterDefaultNumber(n)
      case JsBoolean(b)      => IOParameterDefaultBoolean(b)
      case fileObj: JsObject => IOParameterDefaultFile(DxFile.fromJson(dxApi, fileObj))
      case JsArray(array) =>
        IOParameterDefaultArray(array.map(value => ioParamDefaultFromJs(dxApi, value)))
      case other => throw new Exception(s"Unsupported default value type ${other}")
    }
  }

  def parseIOSpec(dxApi: DxApi, specs: Vector[JsValue]): Vector[IOParameter] = {
    specs.map(ioSpec => parseIoParam(dxApi, ioSpec))
  }
}
