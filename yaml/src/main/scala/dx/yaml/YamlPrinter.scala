package dx.yaml

import org.snakeyaml.engine.v2.api.{Dump, DumpSettings}
import org.snakeyaml.engine.v2.common.{
  FlowStyle => DumperFlowStyle,
  ScalarStyle => DumperScalarStyle
}

sealed trait FlowStyle {
  def toDumperOption: DumperFlowStyle
}

object FlowStyle {
  val Default: FlowStyle = Block
}

case object Auto extends FlowStyle {
  def toDumperOption: DumperFlowStyle = DumperFlowStyle.AUTO
}

case object Block extends FlowStyle {
  def toDumperOption: DumperFlowStyle = DumperFlowStyle.BLOCK
}

case object Flow extends FlowStyle {
  def toDumperOption: DumperFlowStyle = DumperFlowStyle.FLOW
}

sealed trait ScalarStyle {
  def toDumperOption: DumperScalarStyle
}

object ScalarStyle {
  val Default: ScalarStyle = Plain
}

case object DoubleQuoted extends ScalarStyle {
  def toDumperOption: DumperScalarStyle = DumperScalarStyle.DOUBLE_QUOTED
}

case object SingleQuoted extends ScalarStyle {
  def toDumperOption: DumperScalarStyle = DumperScalarStyle.SINGLE_QUOTED
}

case object Literal extends ScalarStyle {
  def toDumperOption: DumperScalarStyle = DumperScalarStyle.LITERAL
}

case object Plain extends ScalarStyle {
  def toDumperOption: DumperScalarStyle = DumperScalarStyle.PLAIN
}

case object Folded extends ScalarStyle {
  def toDumperOption: DumperScalarStyle = DumperScalarStyle.FOLDED
}

sealed trait LineBreak {
  val lineBreak: String
}

object LineBreak {
  val Default: LineBreak = Unix
}

case object Win extends LineBreak {
  val lineBreak = "\r\n"
}

case object Mac extends LineBreak {
  val lineBreak = "\r"
}

case object Unix extends LineBreak {
  val lineBreak = "\n"
}

abstract class YamlPrinter extends (YamlValue => String) {
  def apply(value: YamlValue): String
}

class SnakeYamlPrinter(flowStyle: FlowStyle = FlowStyle.Default,
                       scalarStyle: ScalarStyle = ScalarStyle.Default,
                       lineBreak: LineBreak = LineBreak.Default)
    extends YamlPrinter {
  override def apply(value: YamlValue): String = {
    val settings = DumpSettings
      .builder()
      .setDefaultScalarStyle(scalarStyle.toDumperOption)
      .setDefaultFlowStyle(flowStyle.toDumperOption)
      .setBestLineBreak(lineBreak.lineBreak)
      .build()
    new Dump(settings).dumpToString(value.snakeYamlObject)
  }
}
