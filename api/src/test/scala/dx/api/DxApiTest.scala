package dx.api

import Assumptions.isLoggedIn
import Tags.ApiTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import dx.util.Logger
import spray.json._

class DxApiTest extends AnyFlatSpec with Matchers {
  assume(isLoggedIn)
  private val logger = Logger.Verbose
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
}
