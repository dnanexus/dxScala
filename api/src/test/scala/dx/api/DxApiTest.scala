package dx.api

import Assumptions.{isLoggedIn, toolkitCallable}
import Tags.ApiTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import dx.util.{CommandRunner, FileUtils, Logger}
import org.scalatest.BeforeAndAfterAll
import spray.json._
import org.mockito.MockitoSugar

import java.nio.file.Files
import scala.util.Random

class DxApiTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {
  assume(isLoggedIn)
  assume(toolkitCallable)
  private val logger = Logger.Quiet
  private val dxApi: DxApi = DxApi()(logger)
  private val testProject = "dxCompiler_playground"
  private val publicProject = "dxCompiler_CI"
  private val testRecord = "record-Fgk7V7j0f9JfkYK55P7k3jGY"
  private val testFile = "file-FGqFGBQ0ffPPkYP19gBvFkZy"
  private val foreignFile = "file-FqP0x4Q0bxKXBBXX5pjVYf3Q"
  private val testDatabase = "database-G83KzZQ0yzZv7xK3G1ZJ2p4X"
  private val username = dxApi.whoami()
  private val parentJob = "job-GFG4YxQ0yzZY061b23FKXxZB"
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
    val file = dxApi.file(testFile, Some(dxTestProject))
    file.describe().name shouldBe "fileA"
  }

  it should "find executions and return with correct field descriptors" taggedAs ApiTest in {
    val results = dxApi.findExecutions(
        Map(
            "parentJob" -> JsString(parentJob),
            "describe" -> JsObject(
                "fields" -> DxObject
                  .requestFields(Set(Field.Output, Field.ExecutableName, Field.Details))
            )
        )
    )
    results.fields should not be empty
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

  it should "downloadFile and ignore 503 response when throttles" in {
    val results =
      dxApi.resolveDataObjectBulk(Vector(s"dx://${testProject}:/test_data/fileA"), dxTestProject)
    val dxobj = results.values.head
    val dxFile: DxFile = dxobj.asInstanceOf[DxFile]
    val path = Files.createTempFile(s"${dxFile.id}", ".tmp")
    val dxDownloadCmd =
      s"""dx download ${dxFile.id} -o "${path.toString}" --no-progress -f"""
    val mockRunner = mock[CommandRunner]
    val throttleMsg = "Too many inbound requests, throttling requests for user-my_user, code 503. " +
      "Request Params=blah-blah, Request Other Params=unavailable. Waiting 10000 years before retry..."
    when(mockRunner.execCommand(dxDownloadCmd)) thenReturn ((0, "", throttleMsg))
    dxApi.downloadFile(path, dxFile, overwrite = true, cliRunner = mockRunner) shouldBe ()
    val mockRunnerFailure = mock[CommandRunner]
    val throttleMsgNonExistent =
      "Non-existing throttling message for thrown by the platform, code as if 503"
    when(mockRunnerFailure.execCommand(dxDownloadCmd)) thenReturn ((0, "", throttleMsgNonExistent))
    an[Exception] should be thrownBy dxApi.downloadFile(
        path,
        dxFile,
        overwrite = true,
        cliRunner = mockRunnerFailure,
        retryLimit = 1
    )
  }

  it should "bulk describe files" taggedAs ApiTest in {
    val query = Vector(DxFile(testFile, Some(dxTestProject))(dxApi))
    val result = dxApi.describeFilesBulk(query, validate = true)
    result.size shouldBe 1
    result.head.hasCachedDesc shouldBe true
    result.head.describe().name shouldBe "fileA"
  }

  "DxApi.describeFilesBulk" should "(APPS-1270) describe only the file in the current project if unqualified " +
    "ID is used and ignore files cloned to other projects" taggedAs ApiTest in {
    // File ID exists in another project
    val queryProject1 = Vector(DxFile(testFile, Some(dxApi.resolveProject(publicProject)))(dxApi))
    val result1 =
      dxApi.describeFilesBulk(queryProject1, searchWorkspaceFirst = true, validate = true)
    result1.size shouldBe 1
    result1.head.hasCachedDesc shouldBe true
    result1.head.describe().name shouldBe "fileA"
    // But it gets only one result when describing the file with unqualified ID
    val dxFile = DxFile.fromJson(dxApi, JsObject("$dnanexus_link" -> JsString(testFile)))
    val queryProject2 = Vector(dxFile)
    val result2 =
      dxApi.describeFilesBulk(queryProject2, searchWorkspaceFirst = true, validate = true)
    result2.size shouldBe 1
    result2.head.hasCachedDesc shouldBe true
    result2.head.describe().name shouldBe "fileA"
  }

  it should "describe a database" in {
    val result = dxApi.database(testDatabase, Some(dxTestProject))
    result.describe().name shouldBe "database_a"
  }

  // A dbcluster object in the test project is required to test describe method
  // Here we only test that an exception is thrown when an unexpected object ID is passed
  it should "describe a dbcluster" in {
    assertThrows[Exception] {
      dxApi.dbcluster(testDatabase, Some(dxTestProject))
    }
  }

  it should "bulk describe and fail to validate missing file" taggedAs ApiTest in {
    // Missing file search with explicit project ID
    val query = Vector(DxFile(foreignFile, Some(dxTestProject))(dxApi))
    assertThrows[Exception] {
      dxApi.describeFilesBulk(query, validate = true)
    }
    //  Missing file search without project ID.
    val queryUnqualified = Vector(DxFile(foreignFile, None)(dxApi))
    assertThrows[Exception] {
      dxApi.describeFilesBulk(queryUnqualified, validate = true)
    }
  }

  it should "bulk describe and succeed to validate missing file with fully-qualified ID" taggedAs ApiTest in {
    val query = Vector(DxFile(foreignFile, Some(dxApi.resolveProject(publicProject)))(dxApi))
    val result = dxApi.describeFilesBulk(query, validate = true)
    result.size shouldBe 1
    result.head.hasCachedDesc shouldBe true
    result.head.describe().name shouldBe "test1.test"
  }

  it should "uploadFile and ignore 503 response when throttles" in {
    val dest = s"${dxTestProject.id}:/${uploadPath}/throttled/"
    val fileToUpload = files.keys.head
    val dxUploadCmd =
      s"""dx upload "${fileToUpload.toString}" --brief${dest}"""
    val mockRunner = mock[CommandRunner]
    val throttleMsg = "Too many inbound requests, throttling requests for user-my_user, code 503. " +
      "Request Params=blah-blah, Request Other Params=unavailable. Waiting 10000 years before retry..."
    when(mockRunner.execCommand(dxUploadCmd)) thenReturn ((0, "", throttleMsg))
    an[Exception] should be thrownBy
      dxApi.uploadFile(path = fileToUpload,
                       destination = Some(dest),
                       retryLimit = 1,
                       cliRunner = mockRunner)
    // TODO here to add the test for throttled upload
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
    // test adding a new tag to a file
    val file = results.head._2
    dxApi.addTags(file, Vector("C"), project = Some(dxTestProject))
    val tags = file
      .describeNoCache(Set(Field.Tags))
      .tags
      .getOrElse(throw new Exception("expected file to have tags"))
    tags shouldBe Set("A", "B", "C")
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

  it should "resolve an app with version" in {
    val app = dxApi.resolveApp("bam_to_fastq/1.0.0")
    app.describe().name shouldBe "bam_to_fastq"
    app.describe().version shouldBe "1.0.0"
  }
}
