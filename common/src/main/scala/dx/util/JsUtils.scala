package dx.util

import java.nio.file.Path
import spray.json._

import scala.collection.immutable.TreeMap

object JsUtils {
  def jsFromFile(path: Path): JsValue = {
    FileUtils.readFileContent(path).parseJson
  }

  def jsFromString(json: String): JsValue = {
    json.parseJson
  }

  def jsToString(js: JsValue): String = {
    js.prettyPrint
  }

  def jsToFile(js: JsValue, path: Path): Unit = {
    FileUtils.writeFileContent(path, js.prettyPrint)
  }

  def get(js: JsValue, fieldName: Option[String] = None): JsValue = {
    fieldName.map(x => js.asJsObject.fields(x)).getOrElse(js)
  }

  def getOptional(js: JsValue, fieldName: String): Option[JsValue] = {
    js.asJsObject.fields.get(fieldName)
  }

  def getOptional(fields: Map[String, JsValue], fieldName: String): Option[JsValue] = {
    fields.get(fieldName) match {
      case None | Some(JsNull) => None
      case some                => some
    }
  }

  def getOptionalOrNull(js: JsValue, fieldName: String): JsValue = {
    js.asJsObject.fields.getOrElse(fieldName, JsNull)
  }

  def getOptionalOrNull(fields: Map[String, JsValue], fieldName: String): JsValue = {
    fields.getOrElse(fieldName, JsNull)
  }

  def getFields(js: JsValue, fieldName: Option[String] = None): Map[String, JsValue] = {
    val obj = js match {
      case obj: JsObject => obj
      case other         => other.asJsObject
    }
    fieldName.map(x => obj.fields(x)).getOrElse(obj) match {
      case JsObject(fields) => fields
      case other            => throw new Exception(s"Expected JsObject, got ${other}")
    }
  }

  def getFields(fields: Map[String, JsValue], fieldName: String): Map[String, JsValue] = {
    fields(fieldName) match {
      case JsObject(fields) => fields
      case other            => throw new Exception(s"Expected JsObject, got ${other}")
    }
  }

  def getOptionalFields(js: JsValue, fieldName: String): Option[Map[String, JsValue]] = {
    getOptional(js, fieldName).map(getFields(_))
  }

  def getOptionalFields(fields: Map[String, JsValue],
                        fieldName: String): Option[Map[String, JsValue]] = {
    fields.get(fieldName).map(getFields(_))
  }

  def getValues(js: JsValue, fieldName: Option[String] = None): Vector[JsValue] = {
    get(js, fieldName) match {
      case JsArray(values) => values
      case other           => throw new Exception(s"Expected JsArray, got ${other}")
    }
  }

  def getValues(fields: Map[String, JsValue], fieldName: String): Vector[JsValue] = {
    fields.get(fieldName) match {
      case Some(JsArray(values)) => values
      case other                 => throw new Exception(s"Expected JsArray, got ${other}")
    }
  }

  def getOptionalValues(js: JsValue, fieldName: String): Option[Vector[JsValue]] = {
    getOptional(js, fieldName).map(getValues(_))
  }

  def getOptionalValues(fields: Map[String, JsValue],
                        fieldName: String): Option[Vector[JsValue]] = {
    fields.get(fieldName).map(getValues(_))
  }

  def getString(js: JsValue, fieldName: Option[String] = None): String = {
    get(js, fieldName) match {
      case JsString(value) => value
      case JsNumber(value) => value.toString()
      case other           => throw new Exception(s"Expected a string, got ${other}")
    }
  }

  def getString(fields: Map[String, JsValue], fieldName: String): String = {
    fields.get(fieldName) match {
      case Some(JsString(value)) => value
      case Some(JsNumber(value)) => value.toString()
      case other                 => throw new Exception(s"Expected a string, got ${other}")
    }
  }

  def getOptionalString(js: JsValue, fieldName: String): Option[String] = {
    getOptional(js, fieldName).map(getString(_))
  }

  def getOptionalString(fields: Map[String, JsValue], fieldName: String): Option[String] = {
    fields.get(fieldName).map(getString(_))
  }

  def getInt(js: JsValue, fieldName: Option[String] = None): Int = {
    get(js, fieldName) match {
      case JsNumber(value) => value.toInt
      case JsString(value) => value.toInt
      case other           => throw new Exception(s"Expected a number, got ${other}")
    }
  }

  def getInt(fields: Map[String, JsValue], fieldName: String): Int = {
    fields.get(fieldName) match {
      case Some(JsNumber(value)) => value.toInt
      case Some(JsString(value)) => value.toInt
      case other                 => throw new Exception(s"Expected a number, got ${other}")
    }
  }

  def getOptionalInt(js: JsValue, fieldName: String): Option[Int] = {
    getOptional(js, fieldName).map(getInt(_))
  }

  def getOptionalInt(fields: Map[String, JsValue], fieldName: String): Option[Int] = {
    fields.get(fieldName).map(getInt(_))
  }

  def getLong(js: JsValue, fieldName: Option[String] = None): Long = {
    get(js, fieldName) match {
      case JsNumber(value) => value.toLong
      case JsString(value) => value.toLong
      case other           => throw new Exception(s"Expected a number, got ${other}")
    }
  }

  def getLong(fields: Map[String, JsValue], fieldName: String): Long = {
    fields.get(fieldName) match {
      case Some(JsNumber(value)) => value.toLong
      case Some(JsString(value)) => value.toLong
      case other                 => throw new Exception(s"Expected a number, got ${other}")
    }
  }

  def getOptionalLong(js: JsValue, fieldName: String): Option[Long] = {
    getOptional(js, fieldName).map(getLong(_))
  }

  def getOptionalLong(fields: Map[String, JsValue], fieldName: String): Option[Long] = {
    fields.get(fieldName).map(getLong(_))
  }

  def getDouble(js: JsValue, fieldName: Option[String] = None): Double = {
    get(js, fieldName) match {
      case JsNumber(value) => value.toDouble
      case JsString(value) => value.toDouble
      case other           => throw new Exception(s"Expected a number, got ${other}")
    }
  }

  def getDouble(fields: Map[String, JsValue], fieldName: String): Double = {
    fields.get(fieldName) match {
      case Some(JsNumber(value)) => value.toDouble
      case Some(JsString(value)) => value.toDouble
      case other                 => throw new Exception(s"Expected a number, got ${other}")
    }
  }

  def getOptionalDouble(js: JsValue, fieldName: String): Option[Double] = {
    getOptional(js, fieldName).map(getDouble(_))
  }

  def getOptionalDouble(fields: Map[String, JsValue], fieldName: String): Option[Double] = {
    fields.get(fieldName).map(getDouble(_))
  }

  def getBoolean(js: JsValue, fieldName: Option[String] = None): Boolean = {
    get(js, fieldName) match {
      case JsBoolean(value)  => value
      case JsString("true")  => true
      case JsString("false") => false
      case other             => throw new Exception(s"Expected a boolean, got ${other}")
    }
  }

  def getBoolean(fields: Map[String, JsValue], fieldName: String): Boolean = {
    fields.get(fieldName) match {
      case Some(JsBoolean(value))  => value
      case Some(JsString("true"))  => true
      case Some(JsString("false")) => false
      case other                   => throw new Exception(s"Expected a boolean, got ${other}")
    }
  }

  def getOptionalBoolean(js: JsValue, fieldName: String): Option[Boolean] = {
    getOptional(js, fieldName).map(getBoolean(_))
  }

  def getOptionalBoolean(fields: Map[String, JsValue], fieldName: String): Option[Boolean] = {
    fields.get(fieldName).map(getBoolean(_))
  }

  // Make a JSON value deterministically sorted.  This is used to
  // ensure that the checksum does not change when maps
  // are ordered in different ways.
  //
  // Note: this does not handle the case of arrays that
  // may have different equivalent orderings.
  def makeDeterministic(jsValue: JsValue): JsValue = {
    jsValue match {
      case JsObject(m: Map[String, JsValue]) =>
        // deterministically sort maps by using a tree-map instead
        // a hash-map
        val mTree = m
          .map { case (k, v) => k -> JsUtils.makeDeterministic(v) }
          .to(TreeMap)
        JsObject(mTree)
      case other =>
        other
    }
  }

  // Replace all special json characters from with a white space.
  def sanitizedString(s: String): JsString = {
    def sanitizeChar(ch: Char): String = ch match {
      case '}'                     => " "
      case '{'                     => " "
      case '$'                     => " "
      case '/'                     => " "
      case '\\'                    => " "
      case '\"'                    => " "
      case '\''                    => " "
      case '\n'                    => ch.toString
      case _ if ch.isLetterOrDigit => ch.toString
      case _ if ch.isControl       => " "
      case _                       => ch.toString
    }

    val sanitized: String = if (s != null) {
      s.flatMap(sanitizeChar)
    } else {
      ""
    }

    JsString(sanitized)
  }
}
