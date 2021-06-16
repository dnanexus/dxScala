package dx

package object util {
  def exceptionToString(e: Throwable, brief: Boolean = false): String = {
    if (brief) {
      Iterator
        .iterate(e)(t => t.getCause)
        .takeWhile(_ != null)
        .map(_.getMessage)
        .mkString("\n  caused by: ")
    } else {
      val sw = new java.io.StringWriter
      e.printStackTrace(new java.io.PrintWriter(sw))
      sw.toString
    }
  }

  def errorMessage(message: String,
                   exception: Option[Throwable],
                   stackTrace: Boolean = true): String = {
    (message, exception) match {
      case (s, Some(e)) if s.nonEmpty && stackTrace => s"${s}\n${exceptionToString(e)}"
      case (s, Some(e)) if s.nonEmpty               => s"${s}: ${exceptionToString(e, brief = true)}"
      case ("", Some(e)) if stackTrace              => exceptionToString(e)
      case ("", Some(e))                            => exceptionToString(e, brief = true)
      case (s, None)                                => s
    }
  }

  /**
    * Pretty formats a Scala value similar to its source represention.
    * Particularly useful for case classes.
    * @see https://gist.github.com/carymrobbins/7b8ed52cd6ea186dbdf8
    * @param a The value to pretty print.
    * @param indentSize Number of spaces for each indent.
    * @param maxElementWidth Largest element size before wrapping.
    * @param depth Initial depth to pretty print indents.
    * @return the formatted object as a String
    * TODO: add color
    */
  def prettyFormat(a: Any,
                   indentSize: Int = 2,
                   maxElementWidth: Int = 30,
                   depth: Int = 0,
                   callback: Option[Product => Option[String]] = None): String = {
    val indent = " " * depth * indentSize
    val fieldIndent = indent + (" " * indentSize)
    val nextDepth = prettyFormat(_: Any, indentSize, maxElementWidth, depth + 1, callback)
    a match {
      case s: String =>
        // make Strings look similar to their literal form
        val builder = new StringBuilder
        s.foreach {
          case '\n' => builder.append("\\n")
          case '\r' => builder.append("\\r")
          case '\t' => builder.append("\\t")
          case '"'  => builder.append("\\\"")
          case c    => builder.append(c)
        }
        s""""${builder.toString()}""""
      case xs: Seq[_] if xs.isEmpty =>
        // for an empty Seq just use its normal String representation
        xs.toString()
      case xs: Seq[_] =>
        val resultOneLine = xs.map(nextDepth).toString()
        if (resultOneLine.length <= maxElementWidth) {
          // if the Seq is not too long, pretty print on one line
          resultOneLine
        } else {
          // otherwise, build it with newlines and proper field indents
          val result = xs.map(x => s"\n${fieldIndent}${nextDepth(x)}").toString()
          s"${result.substring(0, result.length - 1)}\n${indent})"
        }
      case Some(x) =>
        s"Some(\n${fieldIndent}${prettyFormat(x, indentSize, maxElementWidth, depth + 1, callback)}\n${indent})"
      case None       => "None"
      case p: Product =>
        // Product includes case classes
        // first check if the callback returns a value
        callback.flatMap(_(p)) match {
          case Some(s) => s
          case _       =>
            // Use reflection to get the constructor arg names and values.
            // Note that this excludes any public values in the case class
            // second parameter list (if any).
            p.productElementNames.zip(p.productIterator).toList match {
              case Nil =>
                // there are no fields, just use toString
                p.toString
              case kvs =>
                // there is more than one field, build up the field names and values
                val prettyFields = kvs.map {
                  case (k, v) => s"${k} = ${nextDepth(v)}"
                }
                // if the result is not too long, pretty print on one line.
                val resultOneLine = s"${p.productPrefix}(${prettyFields.mkString(", ")})"
                if (resultOneLine.length <= maxElementWidth) {
                  resultOneLine
                } else {
                  // Otherwise, build it with newlines and proper field indents.
                  s"${p.productPrefix}(\n${prettyFields.map(f => s"${fieldIndent}${f}").mkString(",\n")}\n${indent})"
                }
            }
        }
      // If we haven't specialized this type, just use its toString.
      case _ => a.toString
    }
  }
}
