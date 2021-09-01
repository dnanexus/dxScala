package dx.yaml

/**
  * Provides the YamlFormats for the most important Scala types.
  */
trait BasicFormats {
  implicit object IntYamlFormat extends YamlFormat[Int] {
    def write(x: Int): YamlNumber = YamlNumber(x)
    def read(value: YamlValue): Int = value match {
      case YamlNumber(x) => x.intValue
      case x =>
        deserializationError("Expected Int as YamlNumber, but got " + x)
    }
  }

  implicit object LongYamlFormat extends YamlFormat[Long] {
    def write(x: Long): YamlNumber = YamlNumber(x)
    def read(value: YamlValue): Long = value match {
      case YamlNumber(x) => x.longValue
      case x =>
        deserializationError("Expected Long as YamlNumber, but got " + x)
    }
  }

  implicit object FloatYamlFormat extends YamlFormat[Float] {
    def write(x: Float): YamlValue = YamlNumber(x)
    def read(value: YamlValue): Float = value match {
      case YamlNumber(x)   => x.floatValue
      case YamlNull        => Float.NaN
      case YamlPositiveInf => Float.PositiveInfinity
      case YamlNegativeInf => Float.NegativeInfinity
      case x =>
        deserializationError("Expected Float as YamlNumber, but got " + x)
    }
  }

  implicit object DoubleYamlFormat extends YamlFormat[Double] {
    def write(x: Double): YamlValue = YamlNumber(x)
    def read(value: YamlValue): Double = value match {
      case YamlNumber(x)   => x.doubleValue
      case YamlNull        => Double.NaN
      case YamlPositiveInf => Double.PositiveInfinity
      case YamlNegativeInf => Double.NegativeInfinity
      case x =>
        deserializationError("Expected Double as YamlNumber, but got " + x)
    }
  }

  implicit object ByteYamlFormat extends YamlFormat[Byte] {
    def write(x: Byte): YamlNumber = YamlNumber(x)
    def read(value: YamlValue): Byte = value match {
      case YamlNumber(x) => x.byteValue
      case x =>
        deserializationError("Expected Byte as YamlNumber, but got " + x)
    }
  }

  implicit object ShortYamlFormat extends YamlFormat[Short] {
    def write(x: Short): YamlNumber = YamlNumber(x)
    def read(value: YamlValue): Short = value match {
      case YamlNumber(x) => x.shortValue
      case x =>
        deserializationError("Expected Short as YamlNumber, but got " + x)
    }
  }

  implicit object BigDecimalYamlFormat extends YamlFormat[BigDecimal] {
    def write(x: BigDecimal): YamlNumber = {
      require(x ne null)
      YamlNumber(x)
    }
    def read(value: YamlValue): BigDecimal = value match {
      case YamlNumber(x) => x
      case x =>
        deserializationError("Expected BigDecimal as YamlNumber, but got " + x)
    }
  }

  implicit object BigIntYamlFormat extends YamlFormat[BigInt] {
    def write(x: BigInt): YamlNumber = {
      require(x ne null)
      YamlNumber(BigDecimal(x))
    }
    def read(value: YamlValue): BigInt = value match {
      case YamlNumber(x) => x.toBigInt
      case x =>
        deserializationError("Expected BigInt as YamlNumber, but got " + x)
    }
  }

  implicit object UnitYamlFormat extends YamlFormat[Unit] {
    def write(x: Unit): YamlNumber = YamlNumber(1)
    def read(value: YamlValue): Unit = {}
  }

  implicit object BooleanYamlFormat extends YamlFormat[Boolean] {
    def write(x: Boolean): YamlBoolean = YamlBoolean(x)
    def read(value: YamlValue): Boolean = value match {
      case YamlBoolean(x) => x
      case x =>
        deserializationError("Expected YamlBoolean, but got " + x)
    }
  }

  implicit object CharYamlFormat extends YamlFormat[Char] {
    def write(x: Char): YamlString = YamlString(String.valueOf(x))
    def read(value: YamlValue): Char = value match {
      case YamlString(x) if x.length == 1 => x.charAt(0)
      case x =>
        deserializationError(
            "Expected Char as single-character YamlString, " +
              "but got " + x
        )
    }
  }

  implicit object StringYamlFormat extends YamlFormat[String] {
    def write(x: String): YamlString = {
      require(x ne null)
      YamlString(x)
    }
    def read(value: YamlValue): String = value match {
      case YamlString(x) => x
      case x =>
        deserializationError("Expected String as YamlString, but got " + x)
    }
  }

  implicit object SymbolYamlFormat extends YamlFormat[Symbol] {
    def write(x: Symbol): YamlString = YamlString(x.name)
    def read(value: YamlValue): Symbol = value match {
      case YamlString(x) => Symbol(x)
      case x =>
        deserializationError("Expected Symbol as YamlString, but got " + x)
    }
  }

//  implicit object DateTimeYamlFormat extends YamlFormat[DateTime] {
//    def write(x: DateTime): YamlDate = YamlDate(x)
//    def read(value: YamlValue): DateTime = value match {
//      case YamlDate(x) => x
//      case x =>
//        deserializationError("Expected Date as YamlDate, but got " + x)
//    }
//  }
}
