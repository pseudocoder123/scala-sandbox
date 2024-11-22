package models.enums

import play.api.libs.json.{Format, JsResult, JsString, JsValue}

object EquipmentCondition extends Enumeration {
  type EquipmentCondition = Value

  val WORKING: Value = Value("WORKING")
  val UNDER_MAINTENANCE: Value = Value("UNDER_MAINTENANCE")
  val DAMAGED: Value = Value("DAMAGED")

  // Implicit Format for EquipmentCondition enum
  implicit val equipmentConditionFormat: Format[EquipmentCondition] = new Format[EquipmentCondition] {
    def reads(json: JsValue): JsResult[EquipmentCondition] = json.validate[String].map(EquipmentCondition.withName)
    def writes(status: EquipmentCondition): JsValue = JsString(status.toString)
  }
}