package dx.util

sealed trait LinkedListInterface[+A] {
  def value: A
  def next: LinkedListInterface[A]
}

case class LinkedList[+A](value: A, next: LinkedListInterface[A]) extends LinkedListInterface[A] {
  override def toString = s"head: $value, next: $next"
}

object LinkedList {
  def create[E](it: Vector[E], acc: LinkedListInterface[E] = LinkedNil): LinkedListInterface[E] = {
    it match {
      case head +: tail => create(tail, LinkedList(head, acc))
      case _            => acc
    }
  }
}
object LinkedNil extends LinkedListInterface[Nothing] {
  def value: Nothing = throw new NoSuchElementException("head of empty list")
  def next: Nothing = throw new UnsupportedOperationException("tail of empty list")
}
