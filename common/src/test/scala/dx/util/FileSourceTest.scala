package dx.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.FileOutputStream
import java.nio.file.{Files, Path}

class FileSourceTest extends AnyFlatSpec with Matchers {
  case object DxProtocol extends FileAccessProtocol {
    val schemes = Vector("dx")
    override def resolve(address: String): AddressableFileNode = ???
  }
  private val resolver = FileSourceResolver.create(userProtocols = Vector(DxProtocol))

  def getProtocol(uriOrPath: String): FileAccessProtocol = {
    val scheme = FileSourceResolver.getScheme(uriOrPath)
    resolver.getProtocolForScheme(scheme)
  }

  it should "Figure out protocols" in {
    // this style of uri will have authority='file-FGqFJ8Q0ffPGVz3zGy4FK02P::' and path='//fileB'
    val proto = getProtocol("dx://file-FGqFJ8Q0ffPGVz3zGy4FK02P:://fileB")
    proto.schemes shouldBe Vector("dx")
  }

  it should "Recognize http" in {
    // recognize http
    val proto = getProtocol("http://A.txt")
    proto.schemes.iterator sameElements Vector("http", "https")

    val proto2 = getProtocol("https://A.txt")
    proto2.schemes.iterator sameElements Vector("http", "https")
  }

  it should "Recognize local files" in {
    // recognize local file access
    val proto = getProtocol("file:///A.txt")
    proto.schemes.iterator sameElements Vector("", "file")
  }

  it should "relativize local paths" in {
    val d = resolver.resolveDirectory("/foo/bar")
    val f1 = resolver.resolve("/foo/bar/baz.txt")
    val f2 = resolver.resolve("/foo/bar/baz/blorf.txt")
    d.relativize(f1) shouldBe "baz.txt"
    d.relativize(f2) shouldBe "baz/blorf.txt"
    f1.relativize(f2) shouldBe "baz/blorf.txt"
  }

  it should "relativize http paths" in {
    val d = resolver.resolveDirectory("http://foo.com/bar/baz")
    val f1 = resolver.resolve("http://foo.com/bar/baz/blorf.txt")
    val f2 = resolver.resolve("http://foo.com/bar/baz/bork/blorf.txt")
    d.relativize(f1) shouldBe "blorf.txt"
    d.relativize(f2) shouldBe "bork/blorf.txt"
    f1.relativize(f2) shouldBe "bork/blorf.txt"
  }

  private def touch(path: Path): Unit = {
    new FileOutputStream(path.toFile).close()
  }

  it should "list a local directory" in {
    val dir = Files.createTempDirectory("temp").toRealPath()
    dir.toFile.deleteOnExit()
    val foo = dir.resolve("foo.txt")
    touch(foo)
    val bar = dir.resolve("bar")
    FileUtils.createDirectories(bar)
    val baz = bar.resolve("baz.txt")
    touch(baz)
    val d = resolver.fromPath(dir)
    val shallowListing = d.listing()
    val deepListing = d.listing(true)
    // make change to subdirectory after generating listings
    val blorf = bar.resolve("blorf.txt")
    touch(blorf)

    // shallow listing should reflect changes to subdirectories
    shallowListing.size shouldBe 2
    val (shallowDirs, shallowFiles) = shallowListing.partition(_.isDirectory)
    shallowFiles.collect {
      case fs: LocalFileSource => fs.canonicalPath
    } shouldBe Vector(foo)
    shallowDirs.size shouldBe 1
    shallowDirs.collect {
      case fs: LocalFileSource => fs.canonicalPath
    } shouldBe Vector(bar)
    shallowDirs.head
      .listing()
      .collect {
        case fs: LocalFileSource => fs.canonicalPath
      }
      .toSet shouldBe Set(baz, blorf)

    // deep listing should not reflect changes to subdirectories
    deepListing.size shouldBe 2
    val (deepDirs, deepFiles) = deepListing.partition(_.isDirectory)
    deepFiles.collect {
      case fs: LocalFileSource => fs.canonicalPath
    } shouldBe Vector(foo)
    deepDirs.size shouldBe 1
    deepDirs.collect {
      case fs: LocalFileSource => fs.canonicalPath
    } shouldBe Vector(bar)
    deepDirs.head
      .listing()
      .collect {
        case fs: LocalFileSource => fs.canonicalPath
      }
      .toSet shouldBe Set(baz)
  }
}
