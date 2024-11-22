package models.request

import play.api.libs.functional.syntax._
import play.api.libs.json._

case class ReturnEquipment(
    allocationId: Long,
    maintenanceRequired: Boolean
)

object ReturnEquipment {
  private val allocationIdReads: Reads[Long] = (JsPath \ "allocationId").read[Long]
  private val maintenanceRequiredReads: Reads[Boolean] = (JsPath \ "maintenanceRequired").read[Boolean]

  // Combine all the reads
  implicit val returnEquipmentReads: Reads[ReturnEquipment] = (
    allocationIdReads and
      maintenanceRequiredReads
    )(ReturnEquipment.apply _)

  // Use Json.writes to generate Writes automatically
  implicit val returnEquipmentWrites: Writes[ReturnEquipment] = Json.writes[ReturnEquipment]

  // Combine Reads and Writes into Format
  implicit val returnEquipmentFormat: Format[ReturnEquipment] = Format(returnEquipmentReads, returnEquipmentWrites)
}



