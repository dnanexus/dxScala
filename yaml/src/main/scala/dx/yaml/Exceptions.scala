package dx.yaml

case class DeserializationException(msg: String,
                                    cause: Throwable = null,
                                    fieldNames: List[String] = Nil)
    extends RuntimeException(msg, cause)

case class SerializationException(msg: String) extends RuntimeException(msg)
