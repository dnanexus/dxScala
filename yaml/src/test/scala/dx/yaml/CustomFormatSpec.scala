package dx.yaml

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CustomFormatSpec extends AnyFlatSpec with Matchers with DefaultYamlProtocol {
  case class MyType(name: String, value: Int)

  implicit val MyTypeProtocol: YF[MyType] = new YamlFormat[MyType] {
    def read(yaml: YamlValue): MyType =
      yaml.asYamlObject.getFields(YamlString("name"), YamlString("value")) match {
        case Seq(YamlString(name), YamlNumber(value)) =>
          MyType(name, value.intValue)
        case _ =>
          deserializationError("Expected fields: 'name' (YAML string) and 'value' (YAML number)")
      }

    def write(obj: MyType): YamlObject =
      YamlObject(YamlString("name") -> YamlString(obj.name),
                 YamlString("value") -> YamlNumber(obj.value))
  }

  {
    val value = MyType("bob", 42)

    "A custom YamlFormat built with 'asYamlObject'" should "correctly deserialize valid YAML content" in {
      """name: bob
        |value: 42""".stripMargin.parseYaml.convertTo[MyType] should ===(value)
    }

    it should "support full round-trip (de)serialization" in {
      value.toYaml.convertTo[MyType] should ===(value)
    }
  }
}
