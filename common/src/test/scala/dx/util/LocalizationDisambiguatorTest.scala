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
}
