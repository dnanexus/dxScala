package dx.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class LocalizationDisambiguatorTest extends AnyFlatSpec with Matchers {
  it should "disambiguate files" in {
    val root = Files.createTempDirectory("root")
    root.toFile.deleteOnExit()
    val disambiguator = SafeLocalizationDisambiguator(root, createDirs = false)
    val fileResolver = FileSourceResolver.get
    val p1 = FileUtils.getPath("/foo/bar.txt")
    val f1 = disambiguator.getLocalPath(fileResolver.fromFile(p1))
    val p2 = FileUtils.getPath("/baz/bar.txt")
    val f2 = disambiguator.getLocalPath(fileResolver.fromFile(p2))
    f1 should not equal f2
  }

  it should "disambiguate files using a common dir" in {
    val root = Files.createTempDirectory("root")
    root.toFile.deleteOnExit()
    val disambiguator = SafeLocalizationDisambiguator(root, createDirs = false)
    val fileResolver = FileSourceResolver.get
    val fs1 = fileResolver.fromFile(FileUtils.getPath("/foo/bar.txt"))
    val fs2 = fileResolver.fromFile(FileUtils.getPath("/baz/bar.txt"))
    val fs3 = fileResolver.fromFile(FileUtils.getPath("/a/b.txt"))
    val fs4 = fileResolver.fromFile(FileUtils.getPath("/c/d.txt"))
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
    val p1 = FileUtils.getPath("/foo/bar.txt")
    val f1 = disambiguator.getLocalPath(fileResolver.fromFile(p1))
    val p2 = FileUtils.getPath("/foo/baz.txt")
    val f2 = disambiguator.getLocalPath(fileResolver.fromFile(p2))
    f1.getParent shouldBe f2.getParent
  }

  it should "disambiguate the same file with different versions" in {
    val root = Files.createTempDirectory("root")
    root.toFile.deleteOnExit()
    val disambiguator = SafeLocalizationDisambiguator(root, createDirs = false)
    val v1Path = disambiguator.localize("foo.txt", "container", Some("1.0"))
    // the first file should not have a version specifier
    v1Path.getParent.getFileName.toString should not be "1.0"
    val v2Path = disambiguator.localize("foo.txt", "container", Some("1.1"))
    // the second file should have a version specifier
    v2Path.getParent.getFileName.toString shouldBe "1.1"
    // a third file with a different name bug same version should be in the same version directory
    val v3Path = disambiguator.localize("bar.txt", "container", Some("1.1"))
    v2Path.getParent shouldBe v3Path.getParent
    // an exact name and version collision should throw an error
    assertThrows[Exception] {
      disambiguator.localize("bar.txt", "container", Some("1.1"))
    }
  }

  it should "throw exception when files with the same name are forced to the same dir" in {
    val root = Files.createTempDirectory("root")
    root.toFile.deleteOnExit()
    val disambiguator = SafeLocalizationDisambiguator(root, createDirs = false)
    val fileResolver = FileSourceResolver.get
    val fs1 = fileResolver.fromFile(FileUtils.getPath("/foo/bar.txt"))
    val fs2 = fileResolver.fromFile(FileUtils.getPath("/baz/bar.txt"))
    val defaultDir = root.resolve("default")
    disambiguator.getLocalPath(fs1, Some(defaultDir))
    assertThrows[Exception] {
      disambiguator.getLocalPath(fs2, Some(defaultDir))
    }
  }
}
