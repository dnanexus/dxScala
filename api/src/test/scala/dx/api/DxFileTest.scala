package dx.api

import Assumptions.isLoggedIn
import Tags.ApiTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._
import dx.util.Logger

class DxFileTest extends AnyFlatSpec with Matchers {
  private val dxApi: DxApi = DxApi()(Logger.Quiet)
  private val testProject: DxProject = dxApi.resolveProject("dxCompiler_playground")
  private val publicProject: DxProject = dxApi.resolveProject("dxCompiler_CI")
  private val FILE_IN_TWO_PROJS: DxFile =
    dxApi.file("file-Fy9VJ1j0yzZgVgFqJPf6KK17", Some(testProject))
  private val FILE_IN_TWO_PROJS_WO_PROJ: DxFile = dxApi.file("file-Fy9VJ1j0yzZgVgFqJPf6KK17", None)
  private val FILE1: DxFile = dxApi.file("file-FGqFGBQ0ffPPkYP19gBvFkZy", Some(testProject))
  private val FILE2: DxFile = dxApi.file("file-FGqFJ8Q0ffPGVz3zGy4FK02P", Some(testProject))
  private val FILE3: DxFile = dxApi.file("file-FGzzpkQ0ffPJX74548Vp6670", Some(testProject))
  private val FILE4: DxFile = dxApi.file("file-FqP0x4Q0bxKXBBXX5pjVYf3Q", Some(publicProject))
  private val FILE5: DxFile = dxApi.file("file-FqP0x4Q0bxKykykX5pVXB1YZ", Some(publicProject))
  private val FILE6: DxFile = dxApi.file("file-Fy9V9000yzZZZZjGJPVVQ4PX", Some(testProject))
  private val FILE6_WO_PROJ: DxFile = dxApi.file("file-Fy9V9000yzZZZZjGJPVVQ4PX", None)
  private val FILE7_WO_PROJ: DxFile = dxApi.file("file-FyQ30800yzZqXXxBBJYxFGF7", None)

  private def checkFileDesc(query: Vector[DxFile],
                            expected: Vector[DxFile],
                            expectedSize: Option[Int] = None,
                            compareDetails: Boolean = false): Unit = {
    assume(isLoggedIn)
    val extraArgs = if (compareDetails) Set(Field.Details) else Set.empty[Field.Value]
    val result = dxApi.describeFilesBulk(query, extraArgs)
    result.size shouldBe expectedSize.getOrElse(expected.size)
    result.forall(_.hasCachedDesc) shouldBe true
    val lookup = DxFileDescCache(expected)
    result.foreach { r =>
      val e: Option[DxFileDescribe] = lookup.getCached(r)
      e shouldBe defined
      r.describe().name shouldBe e.get.name
      r.project.get.id shouldBe e.get.project
      if (compareDetails) {
        r.describe().details shouldBe e.get.details
      }
    }
  }

  def createFile(template: DxFile,
                 name: String,
                 project: Option[DxProject] = None,
                 details: Option[String] = None): DxFile = {
    val proj = project
      .orElse(template.project)
      .getOrElse(
          throw new Exception("no project")
      )
    val file = DxFile(template.id, Some(proj))(dxApi)
    val desc = DxFileDescribe(
        proj.id,
        template.id,
        name,
        null,
        0,
        0,
        0,
        null,
        null,
        details.map(_.parseJson),
        null
    )
    file.cacheDescribe(desc)
    file
  }

  def createFiles(templates: Vector[DxFile],
                  names: Vector[String],
                  projects: Vector[DxProject] = Vector.empty): Vector[DxFile] = {
    templates.zip(names).zipWithIndex.map {
      case ((template, name), i) =>
        val project = if (projects.isEmpty) {
          None
        } else if (projects.size <= i) {
          Some(projects.head)
        } else {
          Some(projects(i))
        }
        createFile(template, name, project)
    }
  }

  it should "bulk describe DxFiles with one project" taggedAs ApiTest in {
    val query = Vector(FILE1, FILE2, FILE3)
    checkFileDesc(query, createFiles(query, Vector("fileA", "fileB", "fileC")))
  }

  it should "bulk describe a file without project" taggedAs ApiTest in {
    val query = Vector(FILE7_WO_PROJ)
    checkFileDesc(query, createFiles(query, Vector("test24.test"), Vector(testProject)))
  }

  it should "bulk describe an empty vector" taggedAs ApiTest in {
    val result = dxApi.describeFilesBulk(Vector.empty)
    result.size shouldBe 0
  }

  it should "bulk describe a duplicate file in vector" taggedAs ApiTest in {
    checkFileDesc(Vector(FILE1, FILE2, FILE1),
                  createFiles(Vector(FILE1, FILE2), Vector("fileA", "fileB")))
  }

  it should "bulk describe a duplicate file in vector2" taggedAs ApiTest in {
    checkFileDesc(Vector(FILE6, FILE6_WO_PROJ),
                  Vector(createFile(FILE6, "test23.test")),
                  expectedSize = Some(2))
  }

  it should "bulk describe files from multiple projects" taggedAs ApiTest in {
    val query = Vector(FILE1, FILE2, FILE5)
    checkFileDesc(query, createFiles(query, Vector("fileA", "fileB", "test2.test")))
  }

  it should "bulk describe files with and without project" taggedAs ApiTest in {
    val query = Vector(FILE4, FILE6_WO_PROJ, FILE7_WO_PROJ)
    checkFileDesc(query,
                  createFiles(query,
                              Vector("test1.test", "test23.test", "test24.test"),
                              Vector(publicProject, testProject, testProject)))
  }

  it should "describe files in bulk with extra fields" taggedAs ApiTest in {
    val expected = Vector(
        createFile(FILE_IN_TWO_PROJS,
                   "File_copied_to_another_project",
                   Some(testProject),
                   Some("{\"detail1\":\"value1\"}")),
        createFile(FILE2, "fileB", Some(testProject), Some("{}"))
    )
    checkFileDesc(Vector(FILE_IN_TWO_PROJS, FILE2), expected, compareDetails = true)
  }

  it should "Describe files in bulk without extra field values - details value should be none" taggedAs ApiTest in {
    val results = dxApi.describeFilesBulk(Vector(FILE_IN_TWO_PROJS, FILE2))
    results.foreach(f => f.describe().details shouldBe None)
  }

  it should "bulk describe file which is in two projects, but projects where to search is given" taggedAs ApiTest in {
    val results = dxApi.describeFilesBulk(Vector(FILE_IN_TWO_PROJS))
    results.forall(_.hasCachedDesc) shouldBe true
    results.size shouldBe 1
  }

  it should "bulk describe file which is in two projects, project where to search is not given" taggedAs ApiTest in {
    val results = dxApi.describeFilesBulk(Vector(FILE_IN_TWO_PROJS_WO_PROJ))
    results.forall(_.hasCachedDesc) shouldBe true
    results.size shouldBe 2
  }
}
