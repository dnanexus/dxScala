package dx.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PosixPathTest extends AnyFlatSpec with Matchers {
  it should "parse an absolute path" in {
    val path = PosixPath("/a/b/c")
    path.toString shouldBe "/a/b/c"
    path.parts shouldBe Vector("a", "b", "c")
    path.nameCount shouldBe 3
    path.isAbsolute shouldBe true
    path.getName shouldBe Some("c")
    val parent = PosixPath(Vector("a", "b"), isAbsolute = true)
    path.getParent shouldBe Some(parent)
    path.startsWith(parent) shouldBe true
    val child = PosixPath(Vector("a", "b", "c", "d"), isAbsolute = true)
    path.resolve("d") shouldBe child
    path.relativize(child) shouldBe PosixPath(Vector("d"), isAbsolute = false)

    if (PosixPath.localFilesystemIsPosix) {
      val javaPath = path.asJavaPath
      path.toString shouldBe javaPath.toString
      path.getName shouldBe Option(javaPath.getFileName).map(_.toString)
      path.isAbsolute shouldBe javaPath.isAbsolute
      path.getParent.map(_.asJavaPath) shouldBe Option(javaPath.getParent)
      path.resolve("d").asJavaPath shouldBe javaPath.resolve("d")
      path.relativize(child).asJavaPath shouldBe javaPath.relativize(child.asJavaPath)
    }
  }

  it should "parse the root path" in {
    val path = PosixPath("/")
    path.toString shouldBe "/"
    path.parts shouldBe empty
    path.nameCount shouldBe 0
    path.isAbsolute shouldBe true
    path.getName shouldBe None
    path.getParent shouldBe None
  }

  it should "parse a relative path" in {
    val path = PosixPath("a/b/c")
    path.toString shouldBe "a/b/c"
    path.parts shouldBe Vector("a", "b", "c")
    path.nameCount shouldBe 3
    path.isAbsolute shouldBe false
    path.getName shouldBe Some("c")
    val parent = PosixPath(Vector("a", "b"), isAbsolute = false)
    path.getParent shouldBe Some(parent)
    path.startsWith(parent) shouldBe true
    val child = PosixPath(Vector("a", "b", "c", "d"), isAbsolute = false)
    path.resolve("d") shouldBe child
    path.relativize(child) shouldBe PosixPath(Vector("d"), isAbsolute = false)

    if (PosixPath.localFilesystemIsPosix) {
      val javaPath = path.asJavaPath
      path.toString shouldBe javaPath.toString
      path.getName shouldBe Option(javaPath.getFileName).map(_.toString)
      path.isAbsolute shouldBe javaPath.isAbsolute
      path.getParent.map(_.asJavaPath) shouldBe Option(javaPath.getParent)
      path.resolve("d").asJavaPath shouldBe javaPath.resolve("d")
      path.relativize(child).asJavaPath shouldBe javaPath.relativize(child.asJavaPath)
    }
  }
}
