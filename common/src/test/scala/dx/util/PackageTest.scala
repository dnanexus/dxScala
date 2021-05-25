package dx.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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
}
