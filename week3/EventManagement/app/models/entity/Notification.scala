package models.entity

import play.api.libs.functional.syntax._
import play.api.libs.json._

import java.time.{LocalDate, LocalDateTime}

case class Notification(
                  id: Option[Long] = None,
                  taskId: Long,
                  teamId: Long,
                  notificationType: String,
                  sentAt: String
                )

object Notification {
  private val idReads: Reads[Option[Long]] = (JsPath \ "id").readNullable[Long]
  private val taskIdReads: Reads[Long] = (JsPath \ "taskId").read[Long]
  private val teamIdReads: Reads[Long] = (JsPath \ "teamId").read[Long]
  private val notificationTypeReads: Reads[String] = (JsPath \ "notificationType").read[String]
  private val sentAtReads: Reads[String] = (JsPath \ "sentAt").read[String]

  // Combine all the reads
  implicit val notificationReads: Reads[Notification] = (
    idReads and
      taskIdReads and
      teamIdReads and
      notificationTypeReads and
      sentAtReads
    )(Notification.apply _)

  // Use Json.writes to generate Writes automatically
  implicit val notificationWrites: Writes[Notification] = Json.writes[Notification]

  // Combine Reads and Writes into Format
  implicit val notificationFormat: Format[Notification] = Format(notificationReads, notificationWrites)
}
