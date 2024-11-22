package models.response

import play.api.libs.functional.syntax._
import play.api.libs.json._
import java.time.LocalDate

case class ReturnEquipmentResponse(
    allocationId: Long,
    equipmentId: Long,
    returnDate: LocalDate
)

object ReturnEquipmentResponse {
  private val allocationIdReads: Reads[Long] = (JsPath \ "allocationId").read[Long]
  private val equipmentIdReads: Reads[Long] = (JsPath \ "equipmentId").read[Long]
  private val returnDateReads: Reads[LocalDate] = (JsPath \ "returnDate").read[LocalDate]

  // Combine all the reads
  implicit val returnEquipmentResponseReads: Reads[ReturnEquipmentResponse] = (
    allocationIdReads and
      equipmentIdReads and
      returnDateReads
    )(ReturnEquipmentResponse.apply _)

  // Use Json.writes to generate Writes automatically
  implicit val returnEquipmentResponseWrites: Writes[ReturnEquipmentResponse] = Json.writes[ReturnEquipmentResponse]

  // Combine Reads and Writes into Format
  implicit val returnEquipmentResponseFormat: Format[ReturnEquipmentResponse] = Format(returnEquipmentResponseReads, returnEquipmentResponseWrites)
}




