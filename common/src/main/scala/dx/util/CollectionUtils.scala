package dx.util

object CollectionUtils {
  implicit class IterableExtensions[A](iterable: Iterable[A]) {
    def asOption: Option[IterableOnce[A]] = {
      if (iterable.nonEmpty) {
        Some(iterable)
      } else {
        None
      }
    }
  }

  implicit class IterableOnceExtensions[A](iterableOnce: IterableOnce[A]) {
    def foldLeftWhile[B](init: B)(where: B => Boolean)(op: (B, A) => B): B = {
      iterableOnce.iterator.foldLeft(init)((acc, next) => if (where(acc)) op(acc, next) else acc)
    }

    def collectFirstDefined[B](mapFunction: A => Option[B]): Option[B] = {
      iterableOnce.iterator.map(mapFunction).collectFirst {
        case x if x.isDefined => x.get
      }
    }

    def mapWithState[B, C](initialState: C)(mapFunction: (C, A) => (C, B)): Iterator[B] = {
      iterableOnce.iterator
        .scanLeft((initialState, Option.empty[B])) {
          case ((state, _), x) =>
            val (newState, result) = mapFunction(state, x)
            (newState, Some(result))
        }
        .map(_._2.get)
    }
  }
}
