package dx.yaml

import java.io.File
import java.net.URLDecoder
import scala.io.Source
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RoundTripSpec extends AnyWordSpec with Matchers {
  def getResourceURL(resource: String): String =
    URLDecoder.decode(getClass.getResource(resource).getFile, "UTF-8")

  "The parsing of YAMLs" should {
    val files = new File(getResourceURL("/examples")).listFiles()
    files.foreach { file =>
      val src = Source.fromFile(file)
      val yamls = src.mkString.parseYamls
      src.close()
      yamls.zipWithIndex.foreach {
        case (innerYaml, i) =>
          s"work in a round trip fashion in ${file} for ${i}" in {
            innerYaml.prettyPrint.parseYaml should ===(innerYaml)
          }
      }
    }
  }
}
