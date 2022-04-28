package dx.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json.{JsNumber, JsObject, JsArray, JsString}

class JsUtilsTest extends AnyFlatSpec with Matchers {
  it should "make JSON maps deterministic" in {
    val x = JsObject("a" -> JsNumber(1), "b" -> JsNumber(2))
    val y = JsObject("b" -> JsNumber(2), "a" -> JsNumber(1))
    JsUtils.makeDeterministic(x) should be(JsUtils.makeDeterministic(y))

    val x2 = JsObject("a" -> JsNumber(10), "b" -> JsNumber(2))
    val y2 = JsObject("b" -> JsNumber(2), "a" -> JsNumber(1))
    assert(JsUtils.makeDeterministic(x2) != JsUtils.makeDeterministic(y2))
  }

  it should "make JSON nested maps deterministic" in {
    val x = JsObject("a" -> JsArray(JsObject("aa" -> JsNumber(2), "ab" -> JsString("aba")),
                                    JsObject("aa" -> JsNumber(2), "ab" -> JsString("abb"))),
                     "b" -> JsNumber(2))

    val y = JsObject(
        "b" -> JsNumber(2),
        //JsObject order changed
        "a" -> JsArray(JsObject("ab" -> JsString("abb"), "aa" -> JsNumber(2)),
                       //JsArray order changed
                       JsObject("aa" -> JsNumber(2), "ab" -> JsString("aba")))
    )
    JsUtils.makeDeterministic(x) should be(JsUtils.makeDeterministic(y))
  }
}
