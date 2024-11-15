package models

import play.api.libs.json._
import play.api.libs.functional.syntax._

import java.time.LocalDate
import java.util.Date

case class AllocationResponse(
                              allocationId: Long,
                              employeeId: Long,
                              approvalRequired: Boolean,
                              equipmentType: String,
                              equipmentId: Option[Long] = None,
                              expectedReturnDate: Option[LocalDate] = None,
                              allocationStatus: String
                            )

object AllocationResponse {
  // Read for the Allocation fields
  private val allocationIdReads: Reads[Long] = (JsPath \ "allocationId").read[Long]
  private val employeeIdReads: Reads[Long] = (JsPath \ "employeeId").read[Long]
  private val approvalRequiredReads: Reads[Boolean] = (JsPath \ "approvalRequired").read[Boolean]
  private val equipmentTypeReads: Reads[String] = (JsPath \ "equipmentType").read[String]
  private val equipmentIdReads: Reads[Option[Long]] = (JsPath \ "equipmentId").readNullable[Long]
  private val expectedReturnDateReads: Reads[Option[LocalDate]] = (JsPath \ "expectedReturnDate").readNullable[LocalDate]
  private val allocationStatus: Reads[String] = (JsPath \ "allocationStatus").read[String]

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



