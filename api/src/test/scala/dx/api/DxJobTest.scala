package dx.api

import Assumptions.isLoggedIn
import Tags.ApiTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import dx.util.Logger

class DxJobTest extends AnyFlatSpec with Matchers {
  assume(isLoggedIn)
  private val dxApi: DxApi = DxApi()(Logger.Quiet)
  private val PlaygroundProject = Some(dxApi.project("project-Fy9QqgQ0yzZbg9KXKP4Jz6Yq"))
  private val AppId = "app-FGybxy00k03JX74548Vp63JV"
  private val AppJobId = "job-G1KzFz00yzZq49Q13zJ7pzX9"
  private val AppletId = "applet-G1vG4F00yzZb69KX20pp0jVP"
  private val AppletJobId = "job-G1vG5Kj0yzZjfjk39zfQVpV2"

  it should "describe job which ran app" taggedAs ApiTest in {
    val dxAppJob = dxApi.job(AppJobId, PlaygroundProject)
    val dxAppJobDescription = dxAppJob.describe(Set(Field.Executable))
    dxAppJobDescription.executable.get.id shouldBe AppId
  }

  it should "describe job which ran applet" taggedAs ApiTest in {
    val dxAppletJob = dxApi.job(AppletJobId, PlaygroundProject)
    val dxAppletJobDescription = dxAppletJob.describe(Set(Field.Executable))
    dxAppletJobDescription.executable.get.id shouldBe AppletId
  }
}
