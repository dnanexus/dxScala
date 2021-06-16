package dx.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class Foo(s1: String, s2: String)(val hidden: Int = 1)
case class Bar(s3: String)
case class Baz(foo: Foo, bar: Bar)

class PackageTest extends AnyFlatSpec with Matchers {
  it should "generate a brief exception message" in {
    try {
      try {
        throw new Exception("exception 1")
      } catch {
        case ex: Exception =>
          throw new Exception("exception 2", ex)
      }
    } catch {
      case ex: Exception =>
        exceptionToString(ex, brief = true) shouldBe "exception 2\n  caused by: exception 1"
    }
  }

  it should "pretty format a case class" in {
    val bar = Baz(Foo("value1", "value2")(), bar = Bar("value3"))
    prettyFormat(bar, maxElementWidth = 26) shouldBe
      """Baz(
        |  foo = Foo(
        |    s1 = "value1",
        |    s2 = "value2"
        |  ),
        |  bar = Bar(s3 = "value3")
        |)""".stripMargin
  }
}
