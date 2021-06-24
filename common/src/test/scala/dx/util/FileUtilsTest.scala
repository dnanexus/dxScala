package dx.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}

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
    val p = Paths.get("/A/./B/../C")
    FileUtils.normalizePath(p).toString shouldBe "/A/C"
  }
}
