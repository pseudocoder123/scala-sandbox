package models.entity

import play.api.libs.functional.syntax._
import play.api.libs.json._

import java.time.LocalDate
import java.util.Date

case class Notifications(
                        id: Option[Long] = None,
                        notificationType: String,
                        allocationId: Long,
                        maintenanceId: Option[Long] = None,
                        createdOn: LocalDate
                      )

object Notifications {
  // Read for the Notification fields
  private val idReads: Reads[Option[Long]] = (JsPath \ "id").readNullable[Long]
  private val notificationTypeReads: Reads[String] = (JsPath \ "notificationType").read[String]
  private val allocationIdReads: Reads[Long] = (JsPath \ "allocationId").read[Long]
  private val maintenanceIdReads: Reads[Option[Long]] = (JsPath \ "maintenanceId").readNullable[Long]
  private val createdOnReads: Reads[LocalDate] = (JsPath \ "createdOn").read[LocalDate]

  // Combine all the reads
  implicit val notificationReads: Reads[Notifications] = (
    idReads and
      notificationTypeReads and
      allocationIdReads and
      maintenanceIdReads and
      createdOnReads
    )(Notifications.apply _)

  // Use Json.writes to generate Writes automatically
  implicit val notificationsWrites: Writes[Notifications] = Json.writes[Notifications]

  // Combine Reads and Writes into Format
  implicit val notificationsFormat: Format[Notifications] = Format(notificationReads, notificationsWrites)
}
