package dx

import org.snakeyaml.engine.v2.api.{Load, LoadSettings}

import scala.jdk.CollectionConverters._

package object yaml {

  // format: OFF
  private[yaml] type YF[A] = YamlFormat[A]
  // format: ON

  def deserializationError(msg: String, cause: Throwable = null, fieldNames: List[String] = Nil) =
    throw DeserializationException(msg, cause, fieldNames)

  def serializationError(msg: String) = throw SerializationException(msg)

  private[yaml] def convertToYamlValue(obj: Object): YamlValue = {
    obj match {
      case m: java.util.Map[Object @unchecked, Object @unchecked] =>
        YamlObject(m.asScala.map {
          case (k, v) => convertToYamlValue(k) -> convertToYamlValue(v)
        }.toMap)
      case l: java.util.List[Object @unchecked] =>
        YamlArray(l.asScala.map(convertToYamlValue).toVector)
      case s: java.util.Set[Object @unchecked] =>
        YamlSet(s.asScala.map(convertToYamlValue).toSet)
      case i: java.lang.Integer    => YamlNumber(i.toInt)
      case i: java.lang.Long       => YamlNumber(i.toLong)
      case i: java.math.BigInteger => YamlNumber(BigInt(i))
      case d: java.lang.Double     => YamlNumber(d.toDouble)
      case s: java.lang.String     => YamlString(s)
      //case d: java.util.Date       => YamlDate(new DateTime(d))
      case b: java.lang.Boolean => YamlBoolean(b)
      case ba: Array[Byte]      => YamlString(new String(ba))
      case null                 => YamlNull
      case other =>
        deserializationError(s"unexpected value ${other}")
    }
  }

  implicit class AnyWithYaml[A](val any: A) extends AnyVal {
    def toYaml(implicit writer: YamlWriter[A]): YamlValue = writer.write(any)
  }

  implicit class StringWithYaml(val string: String) extends AnyVal {
    def parseYaml: YamlValue = parseYaml()

    def parseYaml(allowDuplicateKeys: Boolean = true,
                  allowRecursiveKeys: Boolean = false,
                  maxAliasesForCollections: Int = 50): YamlValue = {
      val loadSettings = LoadSettings
        .builder()
        .setAllowDuplicateKeys(allowDuplicateKeys)
        .setAllowRecursiveKeys(allowRecursiveKeys)
        .setMaxAliasesForCollections(maxAliasesForCollections)
        .build()
      val load = new Load(loadSettings)
      convertToYamlValue(load.loadFromString(string))
    }

    def parseYamls: Seq[YamlValue] = parseYamls()

    def parseYamls(allowDuplicateKeys: Boolean = true,
                   allowRecursiveKeys: Boolean = false,
                   maxAliasesForCollections: Int = 50): Seq[YamlValue] = {
      val loadSettings = LoadSettings
        .builder()
        .setAllowDuplicateKeys(allowDuplicateKeys)
        .setAllowRecursiveKeys(allowRecursiveKeys)
        .setMaxAliasesForCollections(maxAliasesForCollections)
        .build()
      val load = new Load(loadSettings)
      load.loadAllFromString(string).asScala.map(convertToYamlValue).toSeq
    }
  }
}
