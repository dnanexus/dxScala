package dx.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LinkedListInterfaceTest extends AnyFlatSpec with Matchers {
  "LinkedList" should "Return correct value of the second link" in {
    val l1 = LinkedList("A", LinkedNil)
    val l2 = LinkedList("B", l1)
    l2.next.value shouldBe ("A")
  }

  it should "create from vector of integers and return correct value of the third link" in {
    val linkedList = LinkedList.create(Vector(1, 2, 32, 1000))
    linkedList.next.next.next.value shouldBe (1)
    an[NoSuchElementException] should be thrownBy linkedList.next.next.next.next.value
  }
}
