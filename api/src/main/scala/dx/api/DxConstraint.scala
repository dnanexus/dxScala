package dx.api

import spray.json._

sealed abstract class DxConstraintOper(val name: String)

object DxConstraintOper {
  case object And extends DxConstraintOper("$and")
  case object Or extends DxConstraintOper("$or")
}

trait DxConstraint {
  def toJson: JsValue
}

case class DxConstraintString(value: String) extends DxConstraint {
  override def toJson: JsValue = JsString(value)
}

case class DxConstraintKV(key: String, value: String) extends DxConstraint {
  override def toJson: JsValue = JsObject(key -> JsString(value))
}

case class DxConstraintBool(oper: DxConstraintOper, constraints: DxConstraintArray)
    extends DxConstraint {
  override def toJson: JsValue = JsObject(oper.name -> constraints.toJson)
}

case class DxConstraintArray(constraints: Vector[DxConstraint]) extends DxConstraint {
  override def toJson: JsValue = JsArray(constraints.map(_.toJson))
}

object DxConstraintArray {
  def apply(constraints: DxConstraint*): DxConstraintArray = {
    DxConstraintArray(constraints.toVector)
  }
}
