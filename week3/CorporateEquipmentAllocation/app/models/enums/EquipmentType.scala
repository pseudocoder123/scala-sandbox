package models.enums

import play.api.libs.json.{Format, JsResult, JsString, JsValue}

object EquipmentType extends Enumeration {
  type EquipmentType = Value

  val LAPTOP: Value = Value("LAPTOP")
  val TABLET: Value = Value("TABLET")
  val MOUSE: Value = Value("MOUSE")
  val PROJECTOR: Value = Value("PROJECTOR")
  val MONITOR: Value = Value("MONITOR")
  val KEYBOARD: Value = Value("KEYBOARD")

  def isValid(eqType: String): Boolean = {
    values.exists(_.toString == eqType.toUpperCase)
  }

  // Implicit Format for EquipmentType enum
  implicit val equipmentTypeFormat: Format[EquipmentType] = new Format[EquipmentType] {
    def reads(json: JsValue): JsResult[EquipmentType] = json.validate[String].map(EquipmentType.withName)
    def writes(status: EquipmentType): JsValue = JsString(status.toString)
  }
}