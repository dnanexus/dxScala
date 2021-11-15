package dx.util

import spray.json._

import java.io.{FileOutputStream, PrintStream}
import java.nio.file.{Path, Paths}

object LogLevel {
  // show messages at least as severe as INFO
  val Info: Int = 0
  // show messages at least as severe as INFO
  val Warning: Int = 1
  // show messages at least as severe as INFO
  val Error: Int = 2
  // do not show any messages
  val Silent: Int = 3
}

object TraceLevel {
  // show no trace messages
  val None: Int = 0
  // show verbose messages
  val Verbose: Int = 1
  // show extra-verbose messages
  val VVerbose: Int = 2
}

/**
  * Message logger.
  * @param level the minimum level to log
  * @param traceLevel level of trace detail to show - orthogonal to `quiet`, i.e. you can show trace messages
  *                   but not info/warning
  * @param keywords specific keywords for which to enable tracing
  * @param traceIndenting amount to indent trace messages
  * @param logFile file where log messages are written; defaults to stderr
  * @param hideStackTraces whether to hide stack traces for errors; defaults to false if traceLevel >= verbose,
  *                        otherwise true
  */
case class Logger(level: Int,
                  traceLevel: Int,
                  keywords: Set[String] = Set.empty,
                  traceIndenting: Int = 0,
                  logFile: Option[Path] = None,
                  hideStackTraces: Option[Boolean] = None) {
  private val stream: PrintStream = logFile
    .map { path =>
      val fileStream = new PrintStream(new FileOutputStream(path.toFile, true))
      sys.addShutdownHook({
        fileStream.flush()
        fileStream.close()
      })
      fileStream
    }
    .getOrElse(System.err)
  private val DefaultMessageLimit = 1000
  private lazy val keywordsLower: Set[String] = keywords.map(_.toLowerCase)

  lazy val isVerbose: Boolean = traceLevel >= TraceLevel.Verbose

  lazy val showStackTrace: Boolean = hideStackTraces.map(!_).getOrElse(isVerbose)

  // check in a case insensitive fashion
  def containsKey(word: String): Boolean = {
    keywordsLower.contains(word.toLowerCase)
  }

  // returns a Logger that has `verbose = true` if `key` is in `keywords`
  def withTraceIfContainsKey(word: String,
                             newTraceLevel: Int = TraceLevel.Verbose,
                             indentInc: Int = 0): Logger = {
    if (containsKey(word) && newTraceLevel > traceLevel) {
      copy(traceLevel = newTraceLevel, traceIndenting = traceIndenting + indentInc)
    } else {
      this
    }
  }

  // returns a Logger with trace indent increased
  def withIncTraceIndent(steps: Int = 1): Logger = {
    copy(traceIndenting = traceIndenting + steps)
  }

  // print a message with no color - ignored if `quiet` is false
  def info(msg: String): Unit = {
    if (level <= LogLevel.Info) {
      stream.println(msg)
    }
  }

  // print a warning message in yellow - ignored if `quiet` is true and `force` is false
  def warning(msg: String, force: Boolean = false, exception: Option[Throwable] = None): Unit = {
    if (force || level <= LogLevel.Warning) {
      Logger.warning(msg, exception, showStackTrace, stream)
    }
  }

  // print an error message in red
  def error(msg: String, exception: Option[Throwable] = None): Unit = {
    if (level <= LogLevel.Error) {
      Logger.error(msg, exception, showStackTrace, stream)
    }
  }

  private def traceEnabledFor(minLevel: Int, requiredKey: Option[String]): Boolean = {
    traceLevel >= minLevel && requiredKey.forall(containsKey)
  }

  private def printTrace(msg: String, exception: Option[Throwable] = None): Unit = {
    stream.println(errorMessage(s"${" " * traceIndenting * 2}${msg}", exception))
  }

  private def truncateMessage(msg: String,
                              maxLength: Int,
                              showBeginning: Boolean = true,
                              showEnd: Boolean = false): String = {
    if (msg.length <= maxLength || (showBeginning && showEnd && msg.length <= (2 * maxLength))) {
      msg
    } else {
      Vector(
          Option.when(showBeginning)(msg.slice(0, maxLength)),
          Option.when(showEnd)(msg.slice(maxLength - msg.length, maxLength))
      ).flatten.mkString("\n...\n")
    }
  }

  /**
    * Print a detailed message to the user; ignored if `traceLevel` < `level`.
    * @param msg the message to log
    * @param maxLength the max number of characters to show
    * @param minLevel minimum logger trace level required to show this message
    * @param requiredKey logger key required to show this message
    * @param exception exception for which to show stack trace at the end of the log message
    * @param showBeginning if `limit` is defined, whether to show `limit` lines from the beginning of the log
    * @param showEnd if `limit` is defined, whether to show `limit` lines from the end of the log
    */
  def trace(msg: String,
            maxLength: Option[Int] = None,
            minLevel: Int = TraceLevel.Verbose,
            requiredKey: Option[String] = None,
            exception: Option[Throwable] = None,
            showBeginning: Boolean = true,
            showEnd: Boolean = false): Unit = {
    if (traceEnabledFor(minLevel, requiredKey)) {
      if (maxLength.isDefined) {
        printTrace(truncateMessage(msg, maxLength.get, showBeginning, showEnd), exception)
      } else {
        printTrace(msg, exception)
      }
    }
  }

  /**
    * Logging output for applets at runtime. Shortcut for `trace()` with a message `maxLength`
    * (defaults to `APPLET_LOG_MSG_LIMIT`)
    * @param msg the message to log
    * @param limit the max number of characters to show
    * @param minLevel minimum logger trace level required to show this message
    * @param requiredKey logger key required to show this message
    * @param exception exception for which to show stack trace at the end of the log message
    * @param showBeginning if `limit` is defined, whether to show `limit` lines from the beginning of the log
    * @param showEnd if `limit` is defined, whether to show `limit` lines from the end of the log
    */
  def traceLimited(msg: String,
                   limit: Int = DefaultMessageLimit,
                   minLevel: Int = TraceLevel.Verbose,
                   requiredKey: Option[String] = None,
                   exception: Option[Throwable] = None,
                   showBeginning: Boolean = true,
                   showEnd: Boolean = false): Unit = {
    trace(msg, Some(limit), minLevel, requiredKey, exception, showBeginning, showEnd)
  }

  // Ignore a value and print a trace message. This is useful for avoiding warnings/errors
  // on unused variables.
  def ignore[A](value: A,
                minLevel: Int = TraceLevel.VVerbose,
                requiredKey: Option[String] = None): Unit = {
    if (traceEnabledFor(minLevel, requiredKey)) {
      printTrace(s"ignoring ${value}")
    }
  }
}

object Logger {
  lazy val Silent: Logger = Logger(level = LogLevel.Silent, traceLevel = TraceLevel.None)
  lazy val Quiet: Logger = Logger(level = LogLevel.Error, traceLevel = TraceLevel.None)
  lazy val Normal: Logger = Logger(level = LogLevel.Warning, traceLevel = TraceLevel.None)
  lazy val Verbose: Logger = Logger(level = LogLevel.Info, traceLevel = TraceLevel.Verbose)
  private var instance: Logger = Normal

  def get: Logger = instance

  /**
    * Update the system default Logger.
    * @param logger the new default Logger
    * @return the current default Logger
    */
  def set(logger: Logger): Logger = {
    val curDefaultLogger = instance
    instance = logger
    curDefaultLogger
  }

  def set(level: Int,
          traceLevel: Int = TraceLevel.None,
          keywords: Set[String] = Set.empty,
          traceIndenting: Int = 0,
          logFile: Option[Path] = None,
          hideStackTraces: Option[Boolean] = None): Logger = {
    set(Logger(level, traceLevel, keywords, traceIndenting, logFile, hideStackTraces))
  }

  // print a warning message in yellow
  def warning(msg: String,
              exception: Option[Throwable] = None,
              stackTrace: Boolean = false,
              stream: PrintStream = System.err): Unit = {
    stream.println(
        errorMessage(s"${Console.YELLOW}[warning] ${msg}${Console.RESET}", exception, stackTrace)
    )
  }

  // print an error message in red
  def error(msg: String,
            exception: Option[Throwable] = None,
            stackTrace: Boolean = true,
            stream: PrintStream = System.err): Unit = {
    stream.println(
        errorMessage(s"${Console.RED}[error] ${msg}${Console.RESET}", exception, stackTrace)
    )
  }
}

object LoggerProtocol extends DefaultJsonProtocol {
  implicit object PathFormat extends RootJsonFormat[Path] {
    override def read(jsv: JsValue): Path = {
      jsv match {
        case JsString(path) => Paths.get(path)
        case other          => throw new Exception(s"invalid path ${other}")
      }
    }

    override def write(path: Path): JsValue = {
      JsString(path.toString)
    }
  }
  implicit val loggerFormat: RootJsonFormat[Logger] = jsonFormat6(Logger.apply)
}
