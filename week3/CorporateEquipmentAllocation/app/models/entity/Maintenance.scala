package models.entity

import play.api.libs.functional.syntax._
import play.api.libs.json._

import java.time.LocalDate
import java.util.Date

case class Maintenance(
                        id: Option[Long] = None,
                        equipmentId: Long,
                        allocationId: Option[Long] = None,
                        reportedDate: LocalDate,
                        maintenanceStatus: String,
                        serviceCompletedOn: Option[LocalDate] = None,
                        reportedBy: Option[Long] = None
                   )

object Maintenance {
  // Read for the Maintenance fields
  private val idReads: Reads[Option[Long]] = (JsPath \ "id").readNullable[Long]
  private val equipmentIdReads: Reads[Long] = (JsPath \ "equipmentId").read[Long]
  private val allocationIdReads: Reads[Option[Long]] = (JsPath \ "allocationId").readNullable[Long]
  private val reportedDateReads: Reads[LocalDate] = (JsPath \ "reportedDate").read[LocalDate]
  private val maintenanceStatusReads: Reads[String] = (JsPath \ "maintenanceStatus").read[String]
  private val serviceCompletedOnReads: Reads[Option[LocalDate]] = (JsPath \ "serviceCompletedOn").readNullable[LocalDate]
  private val reportedByReads: Reads[Option[Long]] = (JsPath \ "reportedBy").readNullable[Long]

  // Combine all the reads
  implicit val maintenanceReads: Reads[Maintenance] = (
    idReads and
      equipmentIdReads and
      allocationIdReads and
      reportedDateReads and
      maintenanceStatusReads and
      serviceCompletedOnReads and
      reportedByReads
    )(Maintenance.apply _)

  // Use Json.writes to generate Writes automatically
  implicit val maintenanceWrites: Writes[Maintenance] = Json.writes[Maintenance]

  // Combine Reads and Writes into Format
  implicit val maintenanceFormat: Format[Maintenance] = Format(maintenanceReads, maintenanceWrites)
}
