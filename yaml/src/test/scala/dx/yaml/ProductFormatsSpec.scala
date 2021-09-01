package dx.yaml

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.Inside._
import org.scalatest.Inspectors._

class ProductFormatsSpec extends AnyFlatSpec with Matchers {

  case class Test0()
  case class Test2(a: Int, b: Option[Double])
  case class Test3[A, B](as: List[A], bs: List[B])
  case class Test4(t2: Test2)
  case class Test5(a1: Int,
                   a2: Int,
                   a3: Int,
                   a4: Int,
                   a5: Int,
                   a6: Int,
                   a7: Int,
                   a8: Int,
                   a9: Int,
                   a10: Int,
                   a11: Int,
                   a12: Int,
                   a13: Int,
                   a14: Int,
                   a15: Int,
                   a16: Int,
                   a17: Int,
                   a18: Int,
                   a19: Int,
                   a20: Int,
                   a21: Int,
                   a22: Int)
  case class TestTransient(a: Int, b: Option[Double]) {
    @transient var c = false
  }

  @SerialVersionUID(1L) // SerialVersionUID adds a static field to the case class
  case class TestStatic(a: Int, b: Option[Double])
  case class TestMangled(`foo-bar!`: Int,
                         `User ID`: String,
                         `ü$bavf$u56ú$`: Boolean,
                         `-x-`: Int,
                         `=><+-*/!@#%^&~?|`: Float)

  trait TestProtocol extends DefaultYamlProtocol {
    implicit val test0Format: YF[Test0] = yamlFormat0(Test0)
    implicit val test2Format: YF[Test2] = yamlFormat2(Test2)
    implicit def test3Format[A: YamlFormat, B: YamlFormat]: YF[Test3[A, B]] =
      yamlFormat2(Test3.apply[A, B])
    implicit val test4Format: YF[Test4] = yamlFormat1(Test4)
    implicit val test5Format: YF[Test5] = yamlFormat22(Test5)
    implicit val testTransientFormat: YF[TestTransient] = yamlFormat2(TestTransient)
    implicit val testStaticFormat: YF[TestStatic] = yamlFormat2(TestStatic)
    implicit val testMangledFormat: YF[TestMangled] = yamlFormat5(TestMangled)
  }

  object TestProtocol extends TestProtocol
  import TestProtocol._

  {
    val obj = Test2(42, Some(4.2))
    val yaml = YamlObject(YamlString("a") -> YamlNumber(42), YamlString("b") -> YamlNumber(4.2))

    "A YamlFormat created with `yamlFormat`, for a case class with 2 elements," should "convert to a respective YamlObject" in {
      obj.toYaml should ===(yaml)
    }

    it should "convert a YamlObject to the respective case class instance" in {
      yaml.convertTo[Test2] should ===(obj)
    }

    it should "throw a DeserializationException if the YamlObject does not all required members" in {
      a[DeserializationException] should be thrownBy {
        YamlObject(YamlString("b") -> YamlNumber(4.2)).convertTo[Test2]
      }
    }

    it should "not require the presence of optional fields for deserialization" in {
      YamlObject(YamlString("a") -> YamlNumber(42)).convertTo[Test2] should ===(Test2(42, None))
    }

    it should "not render `None` members during serialization" in {
      Test2(42, None).toYaml should ===(YamlObject(YamlString("a") -> YamlNumber(42)))
    }

    it should "ignore additional members during deserialization" in {
      YamlObject(YamlString("a") -> YamlNumber(42),
                 YamlString("b") -> YamlNumber(4.2),
                 YamlString("c") -> YamlString("no")).convertTo[Test2] should ===(obj)
    }

    it should "not depend on any specific member order for deserialization" in {
      YamlObject(YamlString("b") -> YamlNumber(4.2), YamlString("a") -> YamlNumber(42))
        .convertTo[Test2] should ===(obj)
    }

    it should "throw a DeserializationException if the YamlValue is not a YamlObject" in {
      a[DeserializationException] should be thrownBy {
        YamlNull.convertTo[Test2]
      }
    }

    it should "expose the fieldName in the DeserializationException when able" in {
      val thrown = the[DeserializationException] thrownBy { YamlNull.convertTo[Test2] }
      inside(thrown) {
        case DeserializationException(_, _, fieldNames) =>
          fieldNames should ===("a" :: Nil)
      }
    }

    it should "expose all gathered fieldNames in the DeserializationException" in {
      val thrown = the[DeserializationException] thrownBy {
        YamlObject(YamlString("t2") -> YamlObject(YamlString("a") -> YamlString("foo")))
          .convertTo[Test4]
      }
      inside(thrown) {
        case DeserializationException(_, _, fieldNames) =>
          fieldNames should ===("t2" :: "a" :: Nil)
      }
    }
  }

  "A YamlProtocol mixing in NullOptions" should "render `None` members to `null`" in {
    object NullOptionsTestProtocol extends TestProtocol with NullOptions
    import NullOptionsTestProtocol._

    Test2(42, None).toYaml should ===(
        YamlObject(YamlString("a") -> YamlNumber(42), YamlString("b") -> YamlNull)
    )
  }

  {
    val obj = Test3(42 :: 43 :: Nil, "x" :: "y" :: "z" :: Nil)
    val yaml = YamlObject(YamlString("as") -> YamlArray(YamlNumber(42), YamlNumber(43)),
                          YamlString("bs") ->
                            YamlArray(YamlString("x"), YamlString("y"), YamlString("z")))

    "A YamlFormat for a generic case class and created with `yamlFormat`" should "convert to a respective YamlObject" in {
      obj.toYaml should ===(yaml)
    }

    it should "convert a YamlObject to the respective case class instance" in {
      yaml.convertTo[Test3[Int, String]] should ===(obj)
    }
  }

  "A YamlFormat for a generic case class with an explicitly provided type parameter" should "support the yamlFormat1 syntax" in {
    case class Box[A](a: A)
    object BoxProtocol extends DefaultYamlProtocol {
      implicit val boxFormat: YF[Box[Int]] = yamlFormat1(Box[Int])
    }

    import BoxProtocol._
    Box(42).toYaml should ===(YamlObject(YamlString("a") -> YamlNumber(42)))
  }

  {
    val obj = TestTransient(42, Some(4.2))
    val yaml = YamlObject(YamlString("a") -> YamlNumber(42), YamlString("b") -> YamlNumber(4.2))

    "A YamlFormat for a case class with transient fields and created with `yamlFormat`" should "convert to a respective YamlObject" in {
      obj.toYaml should ===(yaml)
    }

    it should "convert a YamlObject to the respective case class instance" in {
      yaml.convertTo[TestTransient] should ===(obj)
    }
  }

  {
    val obj = TestStatic(42, Some(4.2))
    val yaml = YamlObject(YamlString("a") -> YamlNumber(42), YamlString("b") -> YamlNumber(4.2))

    "A YamlFormat for a case class with static fields and created with `yamlFormat`" should "convert to a respective YamlObject" in {
      obj.toYaml should ===(yaml)
    }

    it should "convert a YamlObject to the respective case class instance" in {
      yaml.convertTo[TestStatic] should ===(obj)
    }
  }

  {
    val obj = Test0()
    val yaml = YamlObject()

    "A YamlFormat created with `yamlFormat`, for a case class with 0 elements," should "convert to a respective YamlObject" in {
      obj.toYaml should ===(yaml)
    }

    it should "convert a YamlObject to the respective case class instance" in {
      yaml.convertTo[Test0] should ===(obj)
    }

    it should "ignore additional members during deserialization" in {
      YamlObject(YamlString("a") -> YamlNumber(42)).convertTo[Test0] should ===(obj)
    }

    it should "throw a DeserializationException if the YamlValue is not a YamlObject" in {
      a[DeserializationException] should be thrownBy {
        YamlNull.convertTo[Test0]
      }
    }
  }

  {
    val yaml =
      """ü$bavf$u56ú$: true
        |=><+-*/!@#%^&~?|: 1.0
        |foo-bar!: 42
        |-x-: 26
        |User ID: Karl
        |""".stripMargin

    "A YamlFormat created with `yamlFormat`, for a case class with mangled-name members," should "produce the correct YAML" in {
      val result = TestMangled(42, "Karl", `ü$bavf$u56ú$` = true, 26, 1.0f).toYaml.prettyPrint

      forAll(yaml.split("\n")) { line =>
        result should include(line)
      }
    }

    it should "convert a YamlObject to the respective case class instance" in {
      yaml.parseYaml.convertTo[TestMangled] should ===(
          TestMangled(42, "Karl", `ü$bavf$u56ú$` = true, 26, 1.0f)
      )
    }
  }

  {
    val obj = Test5(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22)
    val yaml = YamlObject(
        YamlString("a1") -> YamlNumber(1),
        YamlString("a2") -> YamlNumber(2),
        YamlString("a3") -> YamlNumber(3),
        YamlString("a4") -> YamlNumber(4),
        YamlString("a5") -> YamlNumber(5),
        YamlString("a6") -> YamlNumber(6),
        YamlString("a7") -> YamlNumber(7),
        YamlString("a8") -> YamlNumber(8),
        YamlString("a9") -> YamlNumber(9),
        YamlString("a10") -> YamlNumber(10),
        YamlString("a11") -> YamlNumber(11),
        YamlString("a12") -> YamlNumber(12),
        YamlString("a13") -> YamlNumber(13),
        YamlString("a14") -> YamlNumber(14),
        YamlString("a15") -> YamlNumber(15),
        YamlString("a16") -> YamlNumber(16),
        YamlString("a17") -> YamlNumber(17),
        YamlString("a18") -> YamlNumber(18),
        YamlString("a19") -> YamlNumber(19),
        YamlString("a20") -> YamlNumber(20),
        YamlString("a21") -> YamlNumber(21),
        YamlString("a22") -> YamlNumber(22)
    )

    "A YamlFormat created with `yamlFormat`, for a case class with 22 elements," should "convert to a respective YamlObject" in {
      obj.toYaml should ===(yaml)
    }

    it should "convert a YamlObject to the respective case class instance" in {
      yaml.convertTo[Test5] should ===(obj)
    }
  }
}
