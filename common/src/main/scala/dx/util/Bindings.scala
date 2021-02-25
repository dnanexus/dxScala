package dx.util

final class DuplicateBindingException(message: String) extends Exception(message)

trait Bindings[K, T] {
  def contains(name: K): Boolean

  def keySet: Set[K]

  def toMap: Map[K, T]

  protected val elementType: String

  protected def copyFrom(values: Map[K, T]): Bindings[K, T]

  def update(bindings: Map[K, T]): Bindings[K, T] = {
    copyFrom(toMap ++ bindings)
  }

  /**
    * Add bindings from another Bindings object to this one. Collisions
    * are not allowed and cause a DuplicateBindingException.
    * @param bindings other bindings
    * @return
    */
  def addAll(bindings: Map[K, T]): Bindings[K, T] = {
    (keySet & bindings.keySet).toVector match {
      case Vector(name) =>
        throw new DuplicateBindingException(
            s"${elementType} ${name} shadows an existing variable"
        )
      case v if v.size > 1 =>
        throw new DuplicateBindingException(
            s"${elementType}s ${v.mkString(",")} shadow existing variables"
        )
      case _ => ()
    }
    update(bindings)
  }

  def addAll(bindings: Bindings[K, T]): Bindings[K, T] = {
    addAll(bindings.toMap)
  }

  def add(key: K, value: T): Bindings[K, T] = {
    addAll(Map(key -> value))
  }

  def apply(key: K): T

  def get(key: K): Option[T]

  def intersect(names: Set[K]): Bindings[K, T] = {
    copyFrom((keySet & names).map(name => name -> apply(name)).toMap)
  }
}

abstract class AbstractBindings[K, T](
    all: Map[K, T] = Map.empty[K, T]
) extends Bindings[K, T] {
  def contains(key: K): Boolean = all.contains(key)

  def keySet: Set[K] = all.keySet

  def toMap: Map[K, T] = all

  def apply(key: K): T = all(key)

  def get(key: K): Option[T] = all.get(key)
}

case class DefaultBindings[T](bindings: Map[String, T] = Map.empty,
                              override val elementType: String = "binding")
    extends AbstractBindings[String, T](bindings) {
  override protected def copyFrom(values: Map[String, T]): DefaultBindings[T] = {
    copy(bindings = values)
  }
}
