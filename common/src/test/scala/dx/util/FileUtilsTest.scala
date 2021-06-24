package dx.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class FileUtilsTest extends AnyFlatSpec with Matchers {
  it should "Correctly replace file suffix" in {
    FileUtils.replaceFileSuffix("foo.bar.baz", ".blorf") shouldBe "foo.bar.blorf"
  }

  it should "create directories" in {
    val root = Files.createTempDirectory("test")
    root.toFile.deleteOnExit()
    val subdir = root.resolve("foo").resolve("bar")
    FileUtils.createDirectories(subdir)
    subdir.toFile.exists() shouldBe true
  }

  it should "normalize a Path" in {
    FileUtils.getPath("/A/./B/../C").toString shouldBe "/A/C"
    FileUtils.getPath("A/../B/./C").toString shouldBe "B/C"
    FileUtils.getPath("../foo/../bar").toString shouldBe "../bar"
    FileUtils.getPath("./hello.txt").toString shouldBe "hello.txt"
  }
}
