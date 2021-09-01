package dx.yaml

import scala.util.{Try, Success, Failure}

/**
  * Provides the YamlFormats for the non-collection standard Scala types
  * (Options, Eithers and Tuples).
  */
trait StandardFormats {

  implicit def optionFormat[A: YF]: YF[Option[A]] = new YF[Option[A]] {

    def write(option: Option[A]): YamlValue = option match {
      case Some(x) => x.toYaml
      case None    => YamlNull
    }

    def read(value: YamlValue): Option[A] = value match {
      case YamlNull => None
      case x        => Some(x.convertTo[A])
    }
  }

  implicit def eitherFormat[A: YF, B: YF]: YF[Either[A, B]] = new YF[Either[A, B]] {

    def write(either: Either[A, B]): YamlValue = either match {
      case Right(a) => a.toYaml
      case Left(b)  => b.toYaml
    }

    def read(value: YamlValue): Either[A, B] =
      (Try(value.convertTo[A]), Try(value.convertTo[B])) match {
        case (Success(a), Failure(_)) => Left(a)
        case (Failure(_), Success(b)) => Right(b)
        case (Success(_), Success(_)) =>
          deserializationError(
              "Ambiguous Either value: can be read as both " +
                "Left and Right values"
          )
        case (Failure(ea), Failure(eb)) =>
          deserializationError(
              "Could not read Either value: " + ea.getMessage
                + " and " + eb.getMessage
          )
      }
  }

  implicit def tuple2Format[A: YF, B: YF]: YF[(A, B)] =
    new YamlFormat[(A, B)] {

      def write(t: (A, B)): YamlArray =
        YamlArray(t._1.toYaml, t._2.toYaml)

      def read(value: YamlValue): (A, B) = value match {
        case YamlArray(Seq(a, b)) =>
          (a.convertTo[A], b.convertTo[B])
        case x =>
          deserializationError("Expected Tuple2 as YamlArray, but got " + x)
      }
    }

  implicit def tuple3Format[A: YF, B: YF, C: YF]: YF[(A, B, C)] =
    new YamlFormat[(A, B, C)] {

      def write(t: (A, B, C)): YamlArray =
        YamlArray(t._1.toYaml, t._2.toYaml, t._3.toYaml)

      def read(value: YamlValue): (A, B, C) = value match {
        case YamlArray(Seq(a, b, c)) =>
          (a.convertTo[A], b.convertTo[B], c.convertTo[C])
        case x =>
          deserializationError("Expected Tuple3 as YamlArray, but got " + x)
      }
    }

  implicit def tuple4Format[A: YF, B: YF, C: YF, D: YF]: YF[(A, B, C, D)] =
    new YamlFormat[(A, B, C, D)] {

      def write(t: (A, B, C, D)): YamlArray =
        YamlArray(t._1.toYaml, t._2.toYaml, t._3.toYaml, t._4.toYaml)

      def read(value: YamlValue): (A, B, C, D) = value match {
        case YamlArray(Seq(a, b, c, d)) =>
          (a.convertTo[A], b.convertTo[B], c.convertTo[C], d.convertTo[D])
        case x =>
          deserializationError("Expected Tuple4 as YamlArray, but got " + x)
      }
    }

  implicit def tuple5Format[A: YF, B: YF, C: YF, D: YF, E: YF]: YF[(A, B, C, D, E)] = {
    new YamlFormat[(A, B, C, D, E)] {

      def write(t: (A, B, C, D, E)): YamlArray =
        YamlArray(t._1.toYaml, t._2.toYaml, t._3.toYaml, t._4.toYaml, t._5.toYaml)

      def read(value: YamlValue): (A, B, C, D, E) = value match {
        case YamlArray(Seq(a, b, c, d, e)) =>
          (a.convertTo[A], b.convertTo[B], c.convertTo[C], d.convertTo[D], e.convertTo[E])
        case x =>
          deserializationError("Expected Tuple5 as YamlArray, but got " + x)
      }
    }
  }

  implicit def tuple6Format[A: YF, B: YF, C: YF, D: YF, E: YF, F: YF]: YF[(A, B, C, D, E, F)] = {
    new YamlFormat[(A, B, C, D, E, F)] {

      def write(t: (A, B, C, D, E, F)): YamlArray =
        YamlArray(t._1.toYaml, t._2.toYaml, t._3.toYaml, t._4.toYaml, t._5.toYaml, t._6.toYaml)

      def read(value: YamlValue): (A, B, C, D, E, F) = value match {
        case YamlArray(Seq(a, b, c, d, e, f)) =>
          (a.convertTo[A],
           b.convertTo[B],
           c.convertTo[C],
           d.convertTo[D],
           e.convertTo[E],
           f.convertTo[F])
        case x =>
          deserializationError("Expected Tuple6 as YamlArray, but got " + x)
      }
    }
  }

  implicit def tuple7Format[A: YF, B: YF, C: YF, D: YF, E: YF, F: YF, G: YF]
      : YF[(A, B, C, D, E, F, G)] = {
    new YamlFormat[(A, B, C, D, E, F, G)] {

      def write(t: (A, B, C, D, E, F, G)): YamlArray =
        YamlArray(t._1.toYaml,
                  t._2.toYaml,
                  t._3.toYaml,
                  t._4.toYaml,
                  t._5.toYaml,
                  t._6.toYaml,
                  t._7.toYaml)

      def read(value: YamlValue): (A, B, C, D, E, F, G) = value match {
        case YamlArray(Seq(a, b, c, d, e, f, g)) =>
          (a.convertTo[A],
           b.convertTo[B],
           c.convertTo[C],
           d.convertTo[D],
           e.convertTo[E],
           f.convertTo[F],
           g.convertTo[G])
        case x =>
          deserializationError("Expected Tuple7 as YamlArray, but got " + x)
      }
    }
  }
}
