package dx.util

sealed abstract class CommandRunnerResponse(val pass: Boolean)

object CommandRunnerResponse {
  final case class CommandSuccess(response: String) extends CommandRunnerResponse(true)
  final case class CommandFailure(response: String) extends CommandRunnerResponse(false)
  final case class CommandApiSuccess(response: String) extends CommandRunnerResponse(true)
  final case class CommandAllowedApiThrottle(response: String) extends CommandRunnerResponse(true)

}
