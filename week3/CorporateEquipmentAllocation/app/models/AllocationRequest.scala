package models

import models.entity.Allocation
import play.api.libs.json._
import play.api.libs.functional.syntax._

case class AllocationRequest(
                       employeeId: Long,
                       equipmentType: String,
                       purpose: Option[String] = None,
                       createdBy: String
                     )

object AllocationRequest {
  // Read for the Allocation fields
  private val employeeIdReads: Reads[Long] = (JsPath \ "employeeId").read[Long]
  private val equipmentTypeReads: Reads[String] = (JsPath \ "equipmentType").read[String]
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



