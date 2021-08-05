package dx.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SysUtilsTest extends AnyFlatSpec with Matchers {
  it should "run a command" in {
    val (_, stdout, _) = SysUtils.execCommand("whoami")
    println(stdout)
  }
}
