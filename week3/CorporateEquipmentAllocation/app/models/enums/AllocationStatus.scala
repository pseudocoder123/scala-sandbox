package models.enums

import play.api.libs.json.{Format, JsResult, JsString, JsValue}

object AllocationStatus extends Enumeration {
  type AllocationStatus = Value

  val APPROVAL_PENDING: Value = Value("APPROVAL_PENDING")
  val REJECTED: Value = Value("REJECTED")
  val REQUESTED: Value = Value("REQUESTED")
  val ALLOCATED: Value = Value("ALLOCATED")
  val NOT_AVAILABLE: Value = Value("NOT_AVAILABLE")
  val OVERDUE_RETURNED: Value = Value("OVERDUE_RETURNED")
  val RETURNED: Value = Value("RETURNED")

  // Implicit Format for AllocationStatus enum
  implicit val allocationStatusFormat: Format[AllocationStatus] = new Format[AllocationStatus] {
    def reads(json: JsValue): JsResult[AllocationStatus] = json.validate[String].map(AllocationStatus.withName)
    def writes(status: AllocationStatus): JsValue = JsString(status.toString)
  }
}