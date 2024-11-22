package models.enums

import play.api.libs.json.{Format, JsResult, JsString, JsValue}

object EventStatus extends Enumeration {
  type EventStatus = Value

  val SCHEDULED: Value = Value("SCHEDULED")
  val REJECTED: Value = Value("REJECTED")
  val CANCELLED: Value = Value("CANCELLED")

  // Implicit Format for EventStatus enum
  implicit val eventTypeFormat: Format[EventStatus] = new Format[EventStatus] {
    def reads(json: JsValue): JsResult[EventStatus] = json.validate[String].map(EventStatus.withName)
    def writes(status: EventStatus): JsValue = JsString(status.toString)
  }

  def withNameOption(name: Option[String]): Option[EventStatus] = name match {
    case Some(name) => name.toUpperCase match {
      case "SCHEDULED" => Some(EventStatus.SCHEDULED)
      case "REJECTED" => Some(EventStatus.REJECTED)
      case "CANCELLED" => Some(EventStatus.CANCELLED)
      case _ => None
    }
    case _ => None
  }
}