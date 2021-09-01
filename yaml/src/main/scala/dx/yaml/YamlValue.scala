package dx.yaml

import scala.jdk.CollectionConverters._

/**
  * The general type of a YAML AST node.
  */
sealed abstract class YamlValue {
  def convertTo[A](implicit reader: YamlReader[A]): A = reader.read(this)

  def asYamlObject(errorMsg: String = "YAML object expected"): YamlObject =
    deserializationError(errorMsg)
  def asYamlObject: YamlObject = asYamlObject()

  private[yaml] def snakeYamlObject: Object

  def prettyPrint: String = print(YamlValue.implicitSnakeYamlPrinter)

  def print(implicit yamlPrinter: YamlPrinter): String = yamlPrinter(this)
}

object YamlValue {
  implicit val implicitSnakeYamlPrinter: SnakeYamlPrinter = new SnakeYamlPrinter
}

/**
  * A YAML mapping from scalars to scalars.
  */
case class YamlObject(fields: Map[YamlValue, YamlValue]) extends YamlValue {
  override def asYamlObject(errorMsg: String): YamlObject = this

  def getFields(fieldKeys: YamlValue*): Seq[YamlValue] =
    fieldKeys.flatMap(fields.get)

  private[yaml] lazy val snakeYamlObject: Object = {
    fields.map {
      case (k, v) =>
        k.snakeYamlObject -> v.snakeYamlObject
    }.asJava
  }
}

object YamlObject {
  def apply(fields: (YamlValue, YamlValue)*) = new YamlObject(Map(fields: _*))
}

/**
  * A YAML array.
  */
case class YamlArray(elements: Vector[YamlValue]) extends YamlValue {
  private[yaml] lazy val snakeYamlObject: Object = {
    elements.map(_.snakeYamlObject).asJava
  }
}

object YamlArray {
  def apply(elements: YamlValue*) = new YamlArray(elements.toVector)
}

/**
  * A YAML set.
  */
case class YamlSet(set: Set[YamlValue]) extends YamlValue {
  private[yaml] lazy val snakeYamlObject: Object = {
    set.map(_.snakeYamlObject).asJava
  }
}

object YamlSet {
  def apply(elements: YamlValue*) = new YamlSet(elements.toSet)
}

/**
  * A YAML string.
  */
case class YamlString(value: String) extends YamlValue {
  private[yaml] lazy val snakeYamlObject = value
}

/**
  * A YAML number
  */
case class YamlNumber(value: BigDecimal) extends YamlValue {
  private[yaml] lazy val snakeYamlObject: Object = {
    value match {
      case v if v.ulp.isWhole => value.underlying.toBigInteger
      case _                  => value
    }
  }
}

object YamlNumber {
  def apply(n: Int) = new YamlNumber(BigDecimal(n))
  def apply(n: Long) = new YamlNumber(BigDecimal(n))
  def apply(n: Double): YamlValue = n match {
    case n if n.isNaN         => YamlNaN
    case n if n.isPosInfinity => YamlPositiveInf
    case n if n.isNegInfinity => YamlNegativeInf
    case _                    => new YamlNumber(BigDecimal(n))
  }
  def apply(n: BigInt) = new YamlNumber(BigDecimal(n))
}

case object YamlNaN extends YamlValue {
  private[yaml] lazy val snakeYamlObject: Object = Double.NaN.asInstanceOf[java.lang.Double]
}

case object YamlPositiveInf extends YamlValue {
  private[yaml] lazy val snakeYamlObject: Object =
    Double.PositiveInfinity.asInstanceOf[java.lang.Double]
}

case object YamlNegativeInf extends YamlValue {
  private[yaml] lazy val snakeYamlObject: Object =
    Double.NegativeInfinity.asInstanceOf[java.lang.Double]
}

///**
//  * A YAML date.
//  */
//case class YamlDate(date: DateTime) extends YamlValue {
//  private[yaml] lazy val snakeYamlObject: Object = date.toDate
//}

/**
  * A YAML boolean.
  */
case class YamlBoolean(boolean: Boolean) extends YamlValue {
  private[yaml] lazy val snakeYamlObject: Object =
    new java.lang.Boolean(boolean)
}

/**
  * The representation for YAML null.
  */
case object YamlNull extends YamlValue {
  private[yaml] lazy val snakeYamlObject: Object = null
}
