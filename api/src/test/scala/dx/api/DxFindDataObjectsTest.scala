package dx.api

import dx.api.Assumptions.isLoggedIn
import dx.api.Tags.ApiTest
import dx.util.Logger
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DxFindDataObjectsTest extends AnyFlatSpec with Matchers {
  assume(isLoggedIn)
  private val logger = Logger.Quiet
  private val dxApi: DxApi = DxApi()(logger)
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

  // set the per-query limit low to test paging
  private lazy val findDataObjects = DxFindDataObjects(dxApi, Some(3))

  it should "find all text files" taggedAs ApiTest in {
    val constraints =
      DxFindDataObjectsConstraints(project = Some(dxTestProject),
                                   folder = Some("/test_data"),
                                   nameGlob = Some("*.txt"))
    val results = findDataObjects.query(constraints)
    results.size shouldBe 6
  }

  it should "find all text files recursively" taggedAs ApiTest in {
    val constraints =
      DxFindDataObjectsConstraints(project = Some(dxTestProject),
                                   folder = Some("/test_data"),
                                   recurse = true,
                                   nameGlob = Some("*.txt"))
    val results = findDataObjects.query(constraints)
    results.size shouldBe 15
  }

  it should "find files with specific names" taggedAs ApiTest in {
    val constraints =
      DxFindDataObjectsConstraints(
          project = Some(dxTestProject),
          folder = Some("/test_data"),
          recurse = true,
          names = Set("hello.2.txt", "hello.py", "whale.txt", "sample_names.txt")
      )
    val results = findDataObjects.query(constraints)
    results.size shouldBe 5
  }

  it should "find files with specific ids" taggedAs ApiTest in {
    val constraints =
      DxFindDataObjectsConstraints(
          project = Some(dxTestProject),
          folder = Some("/test_data"),
          recurse = true,
          ids = Set("file-Fy9V9000yzZZZZjGJPVVQ4PX",
                    "file-G0G3BZQ0yzZV5q4q3vkQJK4V",
                    "file-G284G400yzZgV8xg3vF1jgQp")
      )
    val results = findDataObjects.query(constraints)
    results.size shouldBe 3
  }
}