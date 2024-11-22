package models.request

import play.api.libs.functional.syntax._
import play.api.libs.json._

case class AllocationApprovalRequest(
    allocationId: Long,
    isApproved: Boolean
)

object AllocationApprovalRequest {
  private val allocationIdReads: Reads[Long] = (JsPath \ "allocationId").read[Long]
  private val isApprovedReads: Reads[Boolean] = (JsPath \ "isApproved").read[Boolean]

  // Combine all the reads
  implicit val allocationApprovalRequestReads: Reads[AllocationApprovalRequest] = (
    allocationIdReads and
      isApprovedReads
    )(AllocationApprovalRequest.apply _)

  // Use Json.writes to generate Writes automatically
  implicit val allocationApprovalRequestWrites: Writes[AllocationApprovalRequest] = Json.writes[AllocationApprovalRequest]

  // Combine Reads and Writes into Format
  implicit val allocationApprovalRequestFormat: Format[AllocationApprovalRequest] = Format(allocationApprovalRequestReads, allocationApprovalRequestWrites)
}




