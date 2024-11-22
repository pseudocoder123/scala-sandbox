package models.request

import models.enums.EquipmentType.EquipmentType
import play.api.libs.functional.syntax._
import play.api.libs.json._

case class AllocationRequest(
    employeeId: Long,
    equipmentType: EquipmentType,
    purpose: Option[String] = None,
    createdBy: String
)

object AllocationRequest {
  private val employeeIdReads: Reads[Long] = (JsPath \ "employeeId").read[Long]
  private val equipmentTypeReads: Reads[EquipmentType] = (JsPath \ "equipmentType").read[EquipmentType]
  private val purposeReads: Reads[Option[String]] = (JsPath \ "purpose").readNullable[String]
  private val createdByReads: Reads[String] = (JsPath \ "createdBy").read[String]

  // Combine all the reads
  implicit val allocationRequestReads: Reads[AllocationRequest] = (
    employeeIdReads and
      equipmentTypeReads and
      purposeReads and
      createdByReads
    )(AllocationRequest.apply _)

  // Use Json.writes to generate Writes automatically
  implicit val allocationRequestWrites: Writes[AllocationRequest] = Json.writes[AllocationRequest]

  // Combine Reads and Writes into Format
  implicit val allocationRequestFormat: Format[AllocationRequest] = Format(allocationRequestReads, allocationRequestWrites)
}



