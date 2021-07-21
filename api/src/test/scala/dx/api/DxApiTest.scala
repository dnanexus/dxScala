package dx.api

import Assumptions.{isLoggedIn, toolkitCallable}
import Tags.ApiTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import dx.util.{FileUtils, Logger}
import org.scalatest.BeforeAndAfterAll
import spray.json._

import java.nio.file.Files
import scala.util.Random

class DxApiTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  assume(isLoggedIn)
  assume(toolkitCallable)
  private val logger = Logger.Quiet
  private val dxApi: DxApi = DxApi()(logger)
  private val testProject = "dxCompiler_playground"
  private val testRecord = "record-Fgk7V7j0f9JfkYK55P7k3jGY"
  private val testFile = "file-FGqFGBQ0ffPPkYP19gBvFkZy"

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

  ignore should "describe a record with details" taggedAs ApiTest in {
    val record = dxApi.record(testRecord, Some(dxTestProject))
    record.describe(Set(Field.Details)).details shouldBe Some(
        JsObject(
            "archiveFileId" -> JsObject(
                "$dnanexus_link" -> JsString("file-Fgk7V100f9Jgp3fb5PPp24vg")
            )
        )
    )
  }

  ignore should "describe a file" taggedAs ApiTest in {
    val record = dxApi.file(testFile, Some(dxTestProject))
    record.describe().name shouldBe "fileA"
  }

  it should "resolve a file by name and download bytes" taggedAs ApiTest in {
    val results =
      dxApi.resolveDataObjectBulk(Vector(s"dx://${testProject}:/test_data/fileA"), dxTestProject)
    results.size shouldBe 1
    val dxobj = results.values.head
    val dxFile: DxFile = dxobj.asInstanceOf[DxFile]

    val value = new String(dxApi.downloadBytes(dxFile))
    value shouldBe "The fibonacci series includes 0,1,1,2,3,5\n"
  }

  it should "bulk describe files" taggedAs ApiTest in {
    val query = Vector(DxFile(testFile, Some(dxTestProject))(dxApi))
    val result = dxApi.describeFilesBulk(query, validate = true)
    result.size shouldBe 1
    result.head.hasCachedDesc shouldBe true
    result.head.describe().name shouldBe "fileA"
  }

  it should "bulk describe and fail to validate missing file" taggedAs ApiTest in {
    val query = Vector(DxFile("file-XXXXXXXXXXXXXXXXXXXXXXXX", Some(dxTestProject))(dxApi))
    assertThrows[Exception] {
      dxApi.describeFilesBulk(query, validate = true)
    }
  }

  private val username = dxApi.whoami()
  private val uploadPath = s"unit_tests/${username}/test_upload"
  private val testDir = Files.createTempDirectory("test")
  private val random = new Random(42)
  private val files = Iterator
    .range(0, 5)
    .map { i =>
      val path = testDir.resolve(s"file_${i}.txt")
      val length = random.nextInt(1024 * 10)
      val content = random.nextString(length)
      FileUtils.writeFileContent(path, content)
      val fileSize = content.getBytes().length
      fileSize shouldBe Files.size(path)
      (path, fileSize)
    }
    .toMap
  private val fileTags = Set("A", "B")
  private val fileProperties = Map("name" -> "Joe", "age" -> "42")

  override protected def afterAll(): Unit = {
    dxTestProject.removeFolder(s"/${uploadPath}/", recurse = true, force = true)
  }

  it should "upload files in serial" in {
    val dest = s"${dxTestProject.id}:/${uploadPath}/serial/"
    val uploads = files.keys.map { path =>
      FileUpload(source = path,
                 destination = Some(dest),
                 tags = fileTags,
                 properties = fileProperties)
    }.toSet
    val results = dxApi.uploadFiles(uploads, waitOnUpload = true, parallel = false)
    results.size shouldBe 5
    results.foreach {
      case (path, dxFile) =>
        val desc: DxFileDescribe = dxFile.describe(Set(Field.Tags, Field.Properties))
        files(path) shouldBe desc.size
        desc.tags shouldBe Some(fileTags)
        desc.properties shouldBe Some(fileProperties)
    }
  }

  it should "upload files in parallel" in {
    val dest = s"${dxTestProject.id}:/${uploadPath}/parallel/"
    val uploads = files.keys.map { path =>
      FileUpload(path, Some(dest), fileTags, fileProperties)
    }.toSet
    val results = dxApi.uploadFiles(uploads, waitOnUpload = true, maxConcurrent = 3)
    results.size shouldBe 5
    results.foreach {
      case (path, dxFile) =>
        val desc: DxFileDescribe = dxFile.describe(Set(Field.Tags, Field.Properties))
        files(path) shouldBe desc.size
        desc.tags shouldBe Some(fileTags)
        desc.properties shouldBe Some(fileProperties)
    }
  }
}
