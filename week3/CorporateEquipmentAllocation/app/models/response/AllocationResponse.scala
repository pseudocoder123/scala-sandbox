package models.response

import models.enums.AllocationStatus.AllocationStatus
import models.enums.EquipmentType.EquipmentType
import play.api.libs.functional.syntax._
import play.api.libs.json._
import java.time.LocalDate

case class AllocationResponse(
    allocationId: Long,
    employeeId: Long,
    approvalRequired: Boolean,
    equipmentType: EquipmentType,
    equipmentId: Option[Long] = None,
    expectedReturnDate: Option[LocalDate] = None,
    allocationStatus: AllocationStatus
)

object AllocationResponse {
  private val allocationIdReads: Reads[Long] = (JsPath \ "allocationId").read[Long]
  private val employeeIdReads: Reads[Long] = (JsPath \ "employeeId").read[Long]
  private val approvalRequiredReads: Reads[Boolean] = (JsPath \ "approvalRequired").read[Boolean]
  private val equipmentTypeReads: Reads[EquipmentType] = (JsPath \ "equipmentType").read[EquipmentType]
  private val equipmentIdReads: Reads[Option[Long]] = (JsPath \ "equipmentId").readNullable[Long]
  private val expectedReturnDateReads: Reads[Option[LocalDate]] = (JsPath \ "expectedReturnDate").readNullable[LocalDate]
  private val allocationStatus: Reads[AllocationStatus] = (JsPath \ "allocationStatus").read[AllocationStatus]

  // Combine all the reads
  implicit val allocationResponseReads: Reads[AllocationResponse] = (
    allocationIdReads and
      employeeIdReads and
      approvalRequiredReads and
      equipmentTypeReads and
      equipmentIdReads and
      expectedReturnDateReads and
      allocationStatus
    )(AllocationResponse.apply _)

  // Use Json.writes to generate Writes automatically
  implicit val allocationResponseWrites: Writes[AllocationResponse] = Json.writes[AllocationResponse]

  // Combine Reads and Writes into Format
  implicit val allocationResponseFormat: Format[AllocationResponse] = Format(allocationResponseReads, allocationResponseWrites)
}



