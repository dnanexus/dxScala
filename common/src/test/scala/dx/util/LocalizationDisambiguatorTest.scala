package dx.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}

class LocalizationDisambiguatorTest extends AnyFlatSpec with Matchers {
  it should "disambiguate files" in {
    val root = Files.createTempDirectory("root")
    root.toFile.deleteOnExit()
    val disambiguator = SafeLocalizationDisambiguator(root, createDirs = false)
    val fileResolver = FileSourceResolver.get
    val p1 = Paths.get("/foo/bar.txt")
    val f1 = disambiguator.getLocalPath(fileResolver.fromPath(p1))
    val p2 = Paths.get("/baz/bar.txt")
    val f2 = disambiguator.getLocalPath(fileResolver.fromPath(p2))
    f1 should not equal f2
  }

  it should "disambiguate files using a common dir" in {
    val root = Files.createTempDirectory("root")
    root.toFile.deleteOnExit()
    val disambiguator = SafeLocalizationDisambiguator(root, createDirs = false)
    val fileResolver = FileSourceResolver.get
    val fs1 = fileResolver.fromPath(Paths.get("/foo/bar.txt"))
    val fs2 = fileResolver.fromPath(Paths.get("/baz/bar.txt"))
    val fs3 = fileResolver.fromPath(Paths.get("/a/b.txt"))
    val fs4 = fileResolver.fromPath(Paths.get("/c/d.txt"))
    val localPaths = disambiguator.getLocalPaths(Vector(fs1, fs2, fs3, fs4))
    localPaths(fs1) should not equal localPaths(fs2)
    localPaths(fs1).getParent should not equal localPaths(fs3).getParent
    localPaths(fs3).getParent should equal(localPaths(fs4).getParent)
  }

  it should "place files from the same source container in the same target directory" in {
    val root = Files.createTempDirectory("root")
    root.toFile.deleteOnExit()
    val disambiguator = SafeLocalizationDisambiguator(root, createDirs = false)
    val fileResolver = FileSourceResolver.get
    val p1 = Paths.get("/foo/bar.txt")
    val f1 = disambiguator.getLocalPath(fileResolver.fromPath(p1))
    val p2 = Paths.get("/foo/baz.txt")
    val f2 = disambiguator.getLocalPath(fileResolver.fromPath(p2))
    f1.getParent shouldBe f2.getParent
  }

  it should "disambiguate the same file with different versions" in {
    val root = Files.createTempDirectory("root")
    root.toFile.deleteOnExit()
    val disambiguator = SafeLocalizationDisambiguator(root, createDirs = false)
    val v1Path = disambiguator.getLocalPath("foo.txt", "container", Some("1.0"))
    // the first file should not have a version specifier
    v1Path.getParent.getFileName.toString should not be "1.0"
    val v2Path = disambiguator.getLocalPath("foo.txt", "container", Some("1.1"))
    // the second file should have a version specifier
    v2Path.getParent.getFileName.toString shouldBe "1.1"
    // a third file with a different name bug same version should be in the same version directory
    val v3Path = disambiguator.getLocalPath("bar.txt", "container", Some("1.1"))
    v2Path.getParent shouldBe v3Path.getParent
    // an exact name and version collision should throw an error
    assertThrows[Exception] {
      disambiguator.getLocalPath("bar.txt", "container", Some("1.1"))
    }
  }
}
