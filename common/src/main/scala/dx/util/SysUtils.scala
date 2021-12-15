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
                            stdout: Option[String],
                            stderr: Option[String])
    extends Exception(
        s"""${msg}
           |Command: ${command}
           |Return Code: ${returnCode}
           |STDOUT: ${stdout.getOrElse("")}
           |STDERR: ${stderr.getOrElse("")}""".stripMargin
    )

case class CommandTimeout(msg: String, command: String, timeout: Int) extends Exception(msg)

object SysUtils {
  def runCommand(command: String,
                 timeout: Option[Int] = None,
                 allowedReturnCodes: Set[Int] = Set(0),
                 exceptionOnFailure: Boolean = true,
                 connectInput: Boolean = false,
                 captureStdout: Boolean = true,
                 captureStderr: Boolean = true): (Int, Option[String], Option[String]) = {
    val cmds = Seq("/bin/sh", "-c", command)
    val outStream = Option.when(captureStdout)(new StringBuilder())
    val errStream = Option.when(captureStderr)(new StringBuilder())

    def getStds: (Option[String], Option[String]) = {
      (outStream.map(_.toString), errStream.map(_.toString))
    }

    val fout = outStream
      .map(out => (o: String) => { out.append(o).append("\n"); () })
      .getOrElse((_: String) => ())
    val ferr = errStream
      .map(err => (e: String) => { err.append(e).append("\n"); () })
      .getOrElse((_: String) => ())
    val p: Process = Process(cmds).run(ProcessLogger(fout, ferr), connectInput = connectInput)

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
    val (rc, stdout, stderr) = runCommand(command, timeout, allowedReturnCodes, exceptionOnFailure)
    (rc, stdout.get, stderr.get)
  }

  def execScript(script: Path,
                 timeout: Option[Int] = None,
                 allowedReturnCodes: Set[Int] = Set(0),
                 exceptionOnFailure: Boolean = true): (Int, String, String) = {
    // sh -c executes the commands in 'script' when the argument is a file
    execCommand(script.toString, timeout, allowedReturnCodes, exceptionOnFailure)
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
