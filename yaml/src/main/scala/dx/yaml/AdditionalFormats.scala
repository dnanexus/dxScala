package dx.yaml

/**
  * Provides additional YamlFormats and helpers.
  */
trait AdditionalFormats {

  /**
    * Provides a format for YamlValues directly.
    */
  implicit object YamlValueFormat extends YF[YamlValue] {
    def write(value: YamlValue): YamlValue = value
    def read(value: YamlValue): YamlValue = value
  }

  /**
    * Constructs a YamlFormat from its two parts, YamlReader and YamlWriter.
    */
  def yamlFormat[A](reader: YamlReader[A], writer: YamlWriter[A]): YF[A] = new YF[A] {
    def write(obj: A): YamlValue = writer.write(obj)
    def read(value: YamlValue): A = reader.read(value)
  }

  /**
    * Turns a YamlWriter into a YamlFormat that throws an
    * UnsupportedOperationException for reads.
    */
  def lift[A](writer: YamlWriter[A]): YF[A] = new YF[A] {
    def write(obj: A): YamlValue = writer.write(obj)
    def read(value: YamlValue) =
      throw new UnsupportedOperationException("YamlReader implementation missing")
  }

  /**
    * Turns a YamlReader into a YamlFormat that throws an
    * UnsupportedOperationException for writes.
    */
  def lift[A](reader: YamlReader[A]): YF[A] = new YF[A] {
    def write(obj: A) = throw new UnsupportedOperationException("YamlWriter implementation missing")
    def read(value: YamlValue): A = reader.read(value)
  }

  /**
    * Lazy wrapper around serialization. Useful when you want to serialize
    * (mutually) recursive structures.
    */
  def lazyFormat[A](format: => YF[A]): YF[A] = new YF[A] {
    lazy val delegate: YF[A] = format
    def write(x: A): YamlValue = delegate.write(x)
    def read(value: YamlValue): A = delegate.read(value)
  }
}
