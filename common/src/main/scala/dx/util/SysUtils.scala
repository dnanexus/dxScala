package dx.util

import java.lang.management.ManagementFactory
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import scala.annotation.nowarn
import scala.concurrent.{Await, Future, TimeoutException, blocking, duration}
import scala.concurrent.ExecutionContext.Implicits._
import scala.sys.process.{Process, ProcessLogger}

case class CommandExecError(msg: String,
                            command: String,
                            returnCode: Int,
                            stdout: String,
                            stderr: String)
    extends Exception(
        s"""${msg}
           |Command: ${command}
           |Return Code: ${returnCode}
           |STDOUT: ${stdout}
           |STDERR: ${stderr}""".stripMargin
    )

case class CommandTimeout(msg: String, command: String, timeout: Int) extends Exception(msg)

object SysUtils {
  def execScript(script: Path,
                 timeout: Option[Int] = None,
                 allowedReturnCodes: Set[Int] = Set(0),
                 exceptionOnFailure: Boolean = true): (Int, String, String) = {
    // sh -c executes the commands in 'script' when the argument is a file
    execCommand(script.toString, timeout, allowedReturnCodes, exceptionOnFailure)
  }

  /**
    * Runs a child process using `/bin/sh -c`.
    * @param command the command to run
    * @param timeout seconds to wait before killing the process, or None to wait indefinitely
    * @param allowedReturnCodes Set of valid return codes; devaluts to {0}
    * @param exceptionOnFailure whether to throw an Exception if the command exists with a
    *                           non-zero return code
    */
  def execCommand(command: String,
                  timeout: Option[Int] = None,
                  allowedReturnCodes: Set[Int] = Set(0),
                  exceptionOnFailure: Boolean = true): (Int, String, String) = {
    val cmds = Seq("/bin/sh", "-c", command)
    val outStream = new StringBuilder()
    val errStream = new StringBuilder()
    def getStds: (String, String) = (outStream.toString, errStream.toString)

    val procLogger = ProcessLogger(
        (o: String) => { outStream.append(o).append("\n") },
        (e: String) => { errStream.append(e).append("\n") }
    )
    val p: Process = Process(cmds).run(procLogger, connectInput = false)

    timeout match {
      case Some(nSec) =>
        val f = Future(blocking(p.exitValue()))
        try {
          val retcode = Await.result(f, duration.Duration(nSec, "sec"))
          val (stdout, stderr) = getStds
          (retcode, stdout, stderr)
        } catch {
          case _: TimeoutException =>
            p.destroy()
            throw CommandTimeout(s"Timeout exceeded (${nSec} seconds)", command, nSec)
        }
      case None =>
        // blocks, and returns the exit code. Does NOT connect
        // the standard in of the child job to the parent
        val retcode = p.exitValue()
        val (stdout, stderr) = getStds
        if (exceptionOnFailure && !allowedReturnCodes.contains(retcode)) {
          throw CommandExecError("Error running command", command, retcode, stdout, stderr)
        }
        (retcode, stdout, stderr)
    }
  }

  /**
    * The total memory size.
    * @note this is annotated `nowarn` because it uses a function (getTotalPhysicalMemorySize)
    *       that is deprecated in JDK11 but not replaced until JDK14
    * @return total memory size in bytes
    */
  @nowarn
  def totalMemorySize: Long = {
    val mbean = ManagementFactory.getOperatingSystemMXBean
      .asInstanceOf[com.sun.management.OperatingSystemMXBean]
    mbean.getTotalPhysicalMemorySize
  }

  /**
    * Number of avaialable CPU cores.
    */
  lazy val availableCores: Int = Runtime.getRuntime.availableProcessors()

  /**
    * Times the execution of a block of code.
    * @param block the block to execute
    * @tparam R the return value type
    * @return (return value, time in seconds)
    */
  def time[R](timeUnit: TimeUnit = TimeUnit.SECONDS)(block: => R): (R, Long) = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    (result, timeUnit.convert(t1 - t0, TimeUnit.NANOSECONDS))
  }
}
