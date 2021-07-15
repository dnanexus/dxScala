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
final case class IOParameterValueFile(id: String,
                                      project: Option[String] = None,
                                      name: Option[String] = None)
    extends IOParameterValue {
  def resolve(dxApi: DxApi = DxApi.get): DxFile = {
    DxFile(id, project.map(dxApi.project))(dxApi)
  }
}
final case class DxIoParameterValueFolder(project: String, path: String, name: Option[String])
    extends IOParameterValue

object IOParameterValue {
  def forFileField(field: String,
                   id: Option[String],
                   project: Option[String] = None,
                   name: Option[String] = None,
                   path: Option[String] = None): IOParameterValue = {
    field match {
      case DxIOSpec.Default if id.isDefined && Seq(name, project, path).forall(_.isEmpty) =>
        IOParameterValueFile(id.get)
      case DxIOSpec.Default =>
        throw new Exception("default file value may not have name, project, or path")
      case DxIOSpec.Choices if id.isDefined && path.isEmpty =>
        IOParameterValueFile(id.get, project, name)
      case DxIOSpec.Choices =>
        throw new Exception("choice file value requires a file ID and may not have path")
      case DxIOSpec.Suggestions if id.isDefined && path.isEmpty =>
        IOParameterValueFile(id.get, project, name)
      case DxIOSpec.Suggestions if id.isEmpty && project.isDefined && path.isDefined =>
        DxIoParameterValueFolder(project.get, path.get, name)
      case DxIOSpec.Suggestions =>
        throw new Exception("suggestion file value requires either file ID or project and path")
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
  def parse(dxApi: DxApi, jsv: JsValue): IOParameter = {
    val fields = jsv.asJsObject.fields
    val ioClass = DxIOClass.fromString(JsUtils.getString(fields, DxIOSpec.Class))

    def parseValue(key: String, value: JsValue): IOParameterValue = {
      (ioClass, value) match {
        case (cls, JsArray(array)) if key == DxIOSpec.Default && DxIOClass.isArray(cls) =>
          IOParameterValueArray(array.map(value => parseValue(key, value)))
        case (DxIOClass.File | DxIOClass.FileArray, link @ JsObject(fields))
            if fields.contains(DxUtils.DxLinkKey) =>
          val dxFile = DxFile.fromJson(dxApi, link)
          IOParameterValue.forFileField(key, Some(dxFile.id), dxFile.project.map(_.id))
        case (DxIOClass.File | DxIOClass.FileArray, JsObject(fields)) if key != DxIOSpec.Default =>
          val dxFile = fields.get("value").map {
            case obj @ JsObject(fields) if fields.contains(DxUtils.DxLinkKey) =>
              DxFile.fromJson(dxApi, obj)
            case other =>
              throw new Exception(s"invalid file ${key} value ${other}")
          }
          val project =
            JsUtils.getOptionalString(fields, "project").orElse(dxFile.flatMap(_.project.map(_.id)))
          val name = JsUtils.getOptionalString(fields, "name")
          val path = Option
            .when(key == DxIOSpec.Suggestions)(JsUtils.getOptionalString(fields, "path"))
            .flatten
          IOParameterValue.forFileField(key, dxFile.map(_.id), project, name, path)
        case (DxIOClass.File | DxIOClass.FileArray, JsString(s)) if key != DxIOSpec.Default =>
          val parsed = DxPath.parse(s)
          IOParameterValue.forFileField(key, Some(parsed.name), parsed.projName)
        case (DxIOClass.String | DxIOClass.StringArray, JsString(s)) =>
          IOParameterValueString(s)
        case (DxIOClass.Int | DxIOClass.IntArray, JsNumber(n)) if n.isValidLong =>
          IOParameterValueNumber(n)
        case (DxIOClass.Float | DxIOClass.FloatArray, JsNumber(n)) =>
          IOParameterValueNumber(n)
        case (DxIOClass.Boolean | DxIOClass.BooleanArray, JsBoolean(b)) =>
          IOParameterValueBoolean(b)
        case _ =>
          throw new Exception(s"Unexpected ${key} value ${jsv} of type ${ioClass}")
      }
    }

    def parseType(value: JsValue): DxConstraint = {
      value match {
        case JsString(s) => DxConstraintString(s)
        case JsObject(fields) =>
          if (fields.size != 1) {
            throw new Exception("Constraint hash must have exactly one '$and' or '$or' key")
          }
          fields.head match {
            case (DxConstraintOper.And.name, JsArray(array)) =>
              DxConstraintBool(DxConstraintOper.And, DxConstraintArray(array.map(parseType)))
            case (DxConstraintOper.Or.name, JsArray(array)) =>
              DxConstraintBool(DxConstraintOper.Or, DxConstraintArray(array.map(parseType)))
            case _ =>
              throw new Exception(
                  "Constraint must have key '$and' or '$or' and an array value"
              )
          }
        case _ => throw new Exception(s"Invalid paramter type value ${value}")
      }
    }

    val name = JsUtils.getString(fields, DxIOSpec.Name)
    val optional = JsUtils.getOptionalBoolean(fields, DxIOSpec.Optional).getOrElse(false)
    val group = JsUtils.getOptionalString(fields, DxIOSpec.Group)
    val help = JsUtils.getOptionalString(fields, DxIOSpec.Help)
    val label = JsUtils.getOptionalString(fields, DxIOSpec.Label)
    val patterns = JsUtils.getOptional(fields, DxIOSpec.Patterns).map {
      case JsArray(array) => IOParameterPatternArray(array.map(JsUtils.getString(_)))
      case JsObject(obj) =>
        val name = obj.get("name").map {
          case JsArray(array) => array.map(JsUtils.getString(_))
          case other          => throw new Exception(s"invalid pattern name ${other}")
        }
        val tag = obj.get("tag").map {
          case JsArray(array) => array.map(JsUtils.getString(_))
          case other          => throw new Exception(s"invalid pattern tag ${other}")
        }
        val cls = obj.get("class").map(JsUtils.getString(_))
        IOParameterPatternObject(name, cls, tag)
      case other => throw new Exception(s"invalid patterns ${other}")
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
        case _: Exception =>
          Logger.get.warning(s"unable to parse field ${name} default value ${value}")
          None
      }
    }
    val dxType = JsUtils.getOptional(fields, DxIOSpec.Type).map(parseType)
    IOParameter(
        name = name,
        ioClass = ioClass,
        optional = optional,
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

  def parseIOSpec(dxApi: DxApi, specs: Vector[JsValue]): Vector[IOParameter] = {
    specs.map(ioSpec => parse(dxApi, ioSpec))
  }
}
