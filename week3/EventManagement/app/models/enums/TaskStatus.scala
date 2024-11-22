package models.enums

import play.api.libs.json.{Format, JsResult, JsString, JsValue}

object TaskStatus extends Enumeration {
  type TaskStatus = Value

  val ASSIGNED: Value = Value("ASSIGNED")
  val IN_PROGRESS: Value = Value("IN_PROGRESS")
  val COMPLETED: Value = Value("COMPLETED")
  val CANCELLED: Value = Value("CANCELLED")

  // Implicit Format for TaskStatus enum
  implicit val taskStatusFormat: Format[TaskStatus] = new Format[TaskStatus] {
    def reads(json: JsValue): JsResult[TaskStatus] = json.validate[String].map(TaskStatus.withName)
    def writes(status: TaskStatus): JsValue = JsString(status.toString)
  }

  def withNameOption(name: Option[String]): Option[TaskStatus] = name match {
    case Some(name) => name.toUpperCase match {
      case "ASSIGNED" => Some(TaskStatus.ASSIGNED)
      case "IN_PROGRESS" => Some(TaskStatus.IN_PROGRESS)
      case "COMPLETED" => Some(TaskStatus.COMPLETED)
      case "CANCELLED" => Some(TaskStatus.CANCELLED)
      case _ => None
    }
    case _ => None
  }
}