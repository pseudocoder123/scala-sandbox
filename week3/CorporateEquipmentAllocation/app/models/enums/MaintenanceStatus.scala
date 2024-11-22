package models.enums

import play.api.libs.json.{Format, JsResult, JsString, JsValue}

object MaintenanceStatus extends Enumeration {
  type MaintenanceStatus = Value

  val ISSUE_RAISED: Value = Value("ISSUE_RAISED")
  val IN_SERVICE: Value = Value("IN_SERVICE")
  val WORKING: Value = Value("WORKING")
  val DAMAGED: Value = Value("DAMAGED")

  // Implicit Format for MaintenanceStatus enum
  implicit val maintenanceStatusFormat: Format[MaintenanceStatus] = new Format[MaintenanceStatus] {
    def reads(json: JsValue): JsResult[MaintenanceStatus] = json.validate[String].map(MaintenanceStatus.withName)
    def writes(status: MaintenanceStatus): JsValue = JsString(status.toString)
  }
}