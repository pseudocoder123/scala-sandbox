package models.entity

import models.enums.MaintenanceStatus.MaintenanceStatus
import play.api.libs.functional.syntax._
import play.api.libs.json._
import java.time.LocalDate

case class Maintenance(
    id: Option[Long] = None,
    equipmentId: Long,
    allocationId: Option[Long] = None,
    reportedDate: LocalDate,
    status: MaintenanceStatus,
    completedOn: Option[LocalDate] = None,
    reportedBy: Option[Long] = None
)

object Maintenance {
  // Read for the Maintenance fields
  private val idReads: Reads[Option[Long]] = (JsPath \ "id").readNullable[Long]
  private val equipmentIdReads: Reads[Long] = (JsPath \ "equipmentId").read[Long]
  private val allocationIdReads: Reads[Option[Long]] = (JsPath \ "allocationId").readNullable[Long]
  private val reportedDateReads: Reads[LocalDate] = (JsPath \ "reportedDate").read[LocalDate]
  private val statusReads: Reads[MaintenanceStatus] = (JsPath \ "status").read[MaintenanceStatus]
  private val completedOnReads: Reads[Option[LocalDate]] = (JsPath \ "completedOn").readNullable[LocalDate]
  private val reportedByReads: Reads[Option[Long]] = (JsPath \ "reportedBy").readNullable[Long]

  // Combine all the reads
  implicit val maintenanceReads: Reads[Maintenance] = (
    idReads and
      equipmentIdReads and
      allocationIdReads and
      reportedDateReads and
      statusReads and
      completedOnReads and
      reportedByReads
    )(Maintenance.apply _)

  // Use Json.writes to generate Writes automatically
  implicit val maintenanceWrites: Writes[Maintenance] = Json.writes[Maintenance]

  // Combine Reads and Writes into Format
  implicit val maintenanceFormat: Format[Maintenance] = Format(maintenanceReads, maintenanceWrites)
}
