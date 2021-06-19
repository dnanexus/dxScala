package dx.util.protocols

import dx.api.{DxApi, DxProject}
import dx.util.Assumptions.isLoggedIn
import dx.util.Logger
import dx.util.Tags.ApiTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DxTest extends AnyFlatSpec with Matchers {
  assume(isLoggedIn)
  private val logger = Logger.Quiet
  private val dxApi: DxApi = DxApi()(logger)
  private val proto = DxFileAccessProtocol(dxApi)
  private val testProject = "dxCompiler_playground"
  private lazy val dxTestProject: DxProject = {
    try {
      dxApi.resolveProject(testProject)
    } catch {
      case _: Exception =>
        throw new Exception(
            s"""|Could not find project ${testProject}, you probably need to be logged into
                |the platform on staging.""".stripMargin
        )
    }
  }
  private val testFileName = "/test_data/fileA"
  private val testFileId = "file-FGqFGBQ0ffPPkYP19gBvFkZy"
  private val testFolder = "/test_data/2/"

  it should "resolve a file by ID" taggedAs ApiTest in {
    val f1 = proto.resolve(s"dx://${dxTestProject.id}:${testFileName}")
    val f2 = proto.resolve(s"dx://${dxTestProject.id}:${testFileId}")
    f1.name shouldBe "fileA"
    f1.folder shouldBe "/test_data"
    f1.exists shouldBe true
    f1.isDirectory shouldBe false
    f1.size shouldBe 42
    f1.dxFile.id shouldBe testFileId
    f2.name shouldBe "fileA"
    f2.folder shouldBe "/test_data"
    f2.exists shouldBe true
    f2.isDirectory shouldBe false
    f2.size shouldBe 42
  }

  it should "resolve a folder" taggedAs ApiTest in {
    val d = proto.resolveDirectory(s"dx://${dxTestProject.id}:${testFolder}")
    d.folder shouldBe "/test_data"
    d.name shouldBe "2"
    d.isDirectory shouldBe true
    d.exists shouldBe true
  }

  it should "list a folder" taggedAs ApiTest in {
    val d = proto.resolveDirectory(s"dx://${dxTestProject.id}:${testFolder}")
    // shallow listing
    val shallowListing = d.listing()
    val (shallowFolders, shallowFiles) = shallowListing.partition(_.isDirectory)
    shallowFolders.size shouldBe 1
    shallowFolders.head.name shouldBe "subdir"
    shallowFolders.head match {
      case file: DxFolderSource => file.getParent shouldBe Some(d)
    }
    shallowFiles.size shouldBe 1
    shallowFiles.head.name shouldBe "fileC"
    shallowFiles.head match {
      case file: DxFileSource => file.getParent shouldBe Some(d)
    }
    val deepListing = d.listing(recursive = true)
    val (deepFolders, deepFiles) = deepListing.partition(_.isDirectory)
    deepFolders.size shouldBe 1
    deepFolders.head.name shouldBe "subdir"
    deepFolders.head match {
      case file: DxFolderSource => file.getParent shouldBe Some(d)
    }
    deepFiles.size shouldBe 1
    deepFiles.head.name shouldBe "fileC"
    deepFiles.head match {
      case file: DxFileSource => file.getParent shouldBe Some(d)
    }
    // upload file to see if change is reflected in listing
    val testFile =
      dxApi.uploadString("test", s"${dxTestProject.id}:/test_data/2/subdir/test", wait = true)
    try {
      val shallowFolder = shallowFolders.head
      val shallowSubdirListing = shallowFolder.listing()
      shallowSubdirListing.size shouldBe 2
      shallowSubdirListing.map(_.name).toSet shouldBe Set("fileA", "test")
      shallowSubdirListing.foreach {
        case file: DxFileSource => file.getParent shouldBe Some(shallowFolder)
      }
      val deepFolder = deepFolders.head
      val deepSubdirListing = deepFolder.listing()
      deepSubdirListing.size shouldBe 1
      deepSubdirListing.head.name shouldBe "fileA"
      deepSubdirListing.head match {
        case file: DxFileSource => file.getParent shouldBe Some(deepFolder)
      }
    } finally {
      dxTestProject.removeObjects(Vector(testFile))
    }
  }
}
