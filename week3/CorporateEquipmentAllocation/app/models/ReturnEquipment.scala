package models

import models.entity.Allocation
import play.api.libs.json._
import play.api.libs.functional.syntax._

case class ReturnEquipment(
    allocationId: Long,
    maintenanceRequired: Boolean,
    maintenanceType: Option[String] = None
)

object ReturnEquipment {
  // Read for the Allocation fields
  private val allocationIdReads: Reads[Long] = (JsPath \ "allocationId").read[Long]
  private val maintenanceRequiredReads: Reads[Boolean] = (JsPath \ "maintenanceRequired").read[Boolean]
  private val maintenanceTypeReads: Reads[Option[String]] = (JsPath \ "maintenanceType").readNullable[String]

  // Combine all the reads
  implicit val returnEquipmentReads: Reads[ReturnEquipment] = (
    allocationIdReads and
      maintenanceRequiredReads and
      maintenanceTypeReads
    )(ReturnEquipment.apply _)

  // Use Json.writes to generate Writes automatically
  implicit val returnEquipmentWrites: Writes[ReturnEquipment] = Json.writes[ReturnEquipment]

  // Combine Reads and Writes into Format
  implicit val returnEquipmentFormat: Format[ReturnEquipment] = Format(returnEquipmentReads, returnEquipmentWrites)
}



