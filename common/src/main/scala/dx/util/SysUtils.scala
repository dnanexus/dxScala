package dx.util

import java.lang.management.ManagementFactory
import java.nio.file.Path

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
        (o: String) => { outStream.append(o ++ "\n") },
        (e: String) => { errStream.append(e ++ "\n") }
    )
    val p: Process = Process(cmds).run(procLogger, connectInput = false)

    timeout match {
      case None =>
        // blocks, and returns the exit code. Does NOT connect
        // the standard in of the child job to the parent
        val retcode = p.exitValue()
        val (stdout, stderr) = getStds
        if (!allowedReturnCodes.contains(retcode)) {
          if (exceptionOnFailure) {
            throw CommandExecError("Error running command", command, retcode, stdout, stderr)
          }
        }
        (retcode, stdout, stderr)
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
}
