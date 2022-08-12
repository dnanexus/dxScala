package dx.util

import scala.concurrent.{Await, Future, TimeoutException, blocking, duration}
import scala.concurrent.ExecutionContext.Implicits._
import scala.sys.process.{Process, ProcessLogger}

class CommandRunner {
  private def runCommand(
      command: String,
      timeout: Option[Int],
      allowedReturnCodes: Set[Int],
      exceptionOnFailure: Boolean,
      connectInput: Boolean = false,
      stdoutMode: StdMode.StdMode = StdMode.Capture,
      stderrMode: StdMode.StdMode = StdMode.Capture
  ): (Int, Option[String], Option[String]) = {
    val cmds = Seq("/bin/sh", "-c", command)
    val outStream = Option.when(stdoutMode == StdMode.Capture)(new StringBuilder())
    val errStream = Option.when(stderrMode == StdMode.Capture)(new StringBuilder())

    def getStds: (Option[String], Option[String]) = {
      (outStream.map(_.toString), errStream.map(_.toString))
    }

    val fout = outStream
      .map(out =>
        (o: String) => {
          out.append(o).append("\n"); ()
        }
      )
      .orElse(Option.when(stdoutMode == StdMode.Forward) { (o: String) =>
        sys.process.stdout.append(o).append("\n"); ()
      })
      .getOrElse((_: String) => ())
    val ferr = errStream
      .map(err =>
        (e: String) => {
          err.append(e).append("\n"); ()
        }
      )
      .orElse(Option.when(stderrMode == StdMode.Forward) { (o: String) =>
        sys.process.stderr.append(o).append("\n"); ()
      })
      .getOrElse((_: String) => ())

    val proc: Process = Process(cmds).run(ProcessLogger(fout, ferr), connectInput = connectInput)

    timeout match {
      case Some(nSec) =>
        val f = Future(blocking(proc.exitValue()))
        try {
          val retcode = Await.result(f, duration.Duration(nSec, "sec"))
          val (stdout, stderr) = getStds
          (retcode, stdout, stderr)
        } catch {
          case _: TimeoutException =>
            proc.destroy()
            throw CommandTimeout(s"Timeout exceeded (${nSec} seconds)", command, nSec)
        }
      case None =>
        // blocks, and returns the exit code
        val retcode = proc.exitValue()
        val (stdout, stderr) = getStds
        if (exceptionOnFailure && !allowedReturnCodes.contains(retcode)) {
          throw CommandExecError("Error running command", command, retcode, stdout, stderr)
        }
        (retcode, stdout, stderr)
    }
  }

  /**
    * Runs a child process using `/bin/sh -c` and captures stdout and stderr.
    *
    * @param command            the command to run
    * @param timeout            seconds to wait before killing the process, or None to wait indefinitely
    * @param allowedReturnCodes Set of valid return codes; defaults to {0}
    * @param exceptionOnFailure whether to throw an Exception if the command exists with a
    *                           non-zero return code
    * @return tuple of (return code, stdout, stderr)
    */
  def execCommand(command: String,
                  timeout: Option[Int] = None,
                  allowedReturnCodes: Set[Int] = Set(0),
                  exceptionOnFailure: Boolean = true): (Int, String, String) = {
    val (rc, stdout, stderr) = runCommand(command, timeout, allowedReturnCodes, exceptionOnFailure)
    (rc, stdout.get, stderr.get)
  }
}
