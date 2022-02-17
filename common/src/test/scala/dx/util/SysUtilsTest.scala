package dx.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SysUtilsTest extends AnyFlatSpec with Matchers {
  "SysUtils" should "return code 127, stdout and stderr messages" in {
    val cmd = "(echo 'Hi'; nonexisting-command)"
    val (rc, stdout, stderr) = SysUtils.runCommand(
        command = cmd,
        exceptionOnFailure = false,
        stdoutMode = StdMode.Capture,
        stderrMode = StdMode.Capture
    )
    rc shouldBe (127)
    stdout shouldBe (Some("Hi\n"))
    stderr shouldBe (Some("/bin/sh: nonexisting-command: command not found\n"))
  }

  it should "return code 127, capture and forward stdout and stderr messages to the console" in {
    val cmd = "echo 'Hi'; nonexisting-command"
    val (rc, stdout, stderr) = SysUtils.runCommand(
        command = cmd,
        exceptionOnFailure = false,
        stdoutMode = StdMode.Forward,
        stderrMode = StdMode.Forward
    )
    rc shouldBe (127)
    stdout shouldBe (None)
    stderr shouldBe (None)
  }
}
