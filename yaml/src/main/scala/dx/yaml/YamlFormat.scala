package dx.yaml

import scala.annotation.implicitNotFound
import scala.language.implicitConversions

/**
  * Provides the YAML deserialization for type A.
  */
@implicitNotFound(msg = "Cannot find YamlReader or YamlFormat type class for ${A}")
trait YamlReader[A] {
  def read(yaml: YamlValue): A
}

object YamlReader {
  implicit def func2Reader[A](f: YamlValue => A): YamlReader[A] =
    (yaml: YamlValue) => f(yaml)
}

/**
  * Provides the YAML serialization for type A.
  */
@implicitNotFound(msg = "Cannot find YamlWriter or YamlFormat type class for ${A}")
trait YamlWriter[A] {
  def write(obj: A): YamlValue
}

object YamlWriter {
  implicit def func2Writer[A](f: A => YamlValue): YamlWriter[A] =
    (obj: A) => f(obj)
}

/**
  * Provides the YAML deserialization and serialization for type A.
  */
trait YamlFormat[A] extends YamlReader[A] with YamlWriter[A]
