package dx.yaml

import scala.collection.immutable
import scala.collection.immutable.LinearSeq
import scala.reflect.ClassTag

/**
  * Provides the YamlFormats for the most important Scala collections.
  */
trait CollectionFormats {

  /**
    * Supplies the YamlFormat for Lists.
    */
  implicit def listFormat[A: YF]: YF[List[A]] = new YF[List[A]] {
    def write(list: List[A]): YamlArray = YamlArray(list.map(_.toYaml).toVector)
    def read(value: YamlValue): List[A] = value match {
      case YamlArray(elements) => elements.map(_.convertTo[A]).toList
      case x =>
        deserializationError("Expected List as YamlArray, but got " + x)
    }
  }

  /**
    * Supplies the YamlFormat for Arrays.
    */
  implicit def arrayFormat[A: YF: ClassTag]: YF[Array[A]] = new YF[Array[A]] {
    def write(array: Array[A]): YamlValue = YamlArray(array.map(_.toYaml).toVector)
    def read(value: YamlValue): Array[A] = value match {
      case YamlArray(elements) => elements.map(_.convertTo[A]).toArray
      case x =>
        deserializationError("Expected Array as YamlArray, but got " + x)
    }
  }

  /**
    * Supplies the YamlFormat for Sets.
    */
  implicit def setFormat[A: YF]: YF[Set[A]] = new YF[Set[A]] {
    def write(set: Set[A]): YamlValue = YamlSet(set.map(_.toYaml))
    def read(value: YamlValue): Set[A] = value match {
      case YamlSet(elements) => elements.map(_.convertTo[A])
      case x =>
        deserializationError("Expected Set as YamlSet, but got " + x)
    }
  }

  /**
    * Supplies the YamlFormat for Maps.
    */
  implicit def mapFormat[K: YF, V: YF]: YF[Map[K, V]] = new YF[Map[K, V]] {
    def write(m: Map[K, V]): YamlValue =
      YamlObject(m.map { case (k, v) => k.toYaml -> v.toYaml })

    def read(value: YamlValue): Map[K, V] = value match {
      case x: YamlObject =>
        x.fields.map { case (k, v) => k.convertTo[K] -> v.convertTo[V] }
      case x =>
        deserializationError("Expected Map as YamlObject, but got " + x)
    }
  }

  import collection.{immutable => imm}

  implicit def immIterableFormat[T: YF]: YF[immutable.Iterable[T]] =
    viaSeq[imm.Iterable[T], T](seq => imm.Iterable(seq: _*))

  implicit def immSeqFormat[T: YF]: YF[Seq[T]] =
    viaSeq[imm.Seq[T], T](seq => imm.Seq(seq: _*))

  implicit def immIndexedSeqFormat[T: YF]: YF[IndexedSeq[T]] =
    viaSeq[imm.IndexedSeq[T], T](seq => imm.IndexedSeq(seq: _*))

  implicit def immLinearSeqFormat[T: YF]: YF[LinearSeq[T]] =
    viaSeq[imm.LinearSeq[T], T](seq => imm.LinearSeq(seq: _*))

  implicit def vectorFormat[T: YF]: YF[Vector[T]] =
    viaSeq[Vector[T], T](seq => Vector(seq: _*))

  implicit def iterableFormat[T: YF]: YF[Iterable[T]] =
    viaSeq[Iterable[T], T](seq => Iterable(seq: _*))

  implicit def seqFormat[T: YF]: YF[Seq[T]] =
    viaSeq[Seq[T], T](seq => Seq(seq: _*))

  implicit def indexedSeqFormat[T: YF]: YF[IndexedSeq[T]] =
    viaSeq[IndexedSeq[T], T](seq => IndexedSeq(seq: _*))

  implicit def linearSeqFormat[T: YF]: YF[LinearSeq[T]] =
    viaSeq[LinearSeq[T], T](seq => LinearSeq(seq: _*))

  /**
    * A YamlFormat construction helper that creates a YamlFormat for an Iterable
    * type I from a builder function List => I
    */
  def viaSeq[I <: Iterable[T], T: YF](f: imm.Seq[T] => I): YF[I] = new YF[I] {
    def write(iterable: I): YamlValue = YamlArray(iterable.map(_.toYaml).toVector)
    def read(value: YamlValue): I = value match {
      case YamlArray(elements) => f(elements.map(_.convertTo[T]))
      case x =>
        deserializationError("Expected Collection as YamlArray, but got " + x)
    }
  }
}
