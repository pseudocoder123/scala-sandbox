package models

import models.entity.Allocation
import play.api.libs.json._
import play.api.libs.functional.syntax._

case class MaintenanceUpdateRequest(
                                      maintenanceId: Long,
                                      isWorking: Boolean
                                    )

object MaintenanceUpdateRequest {
  // Read for the Allocation fields
  private val maintenanceIdReads: Reads[Long] = (JsPath \ "maintenanceId").read[Long]
  private val isWorkingReads: Reads[Boolean] = (JsPath \ "isWorking").read[Boolean]

  // Combine all the reads
  implicit val maintenanceUpdateRequestReads: Reads[MaintenanceUpdateRequest] = (
    maintenanceIdReads and
      isWorkingReads
    )(MaintenanceUpdateRequest.apply _)

  // Use Json.writes to generate Writes automatically
  implicit val maintenanceUpdateRequestWrites: Writes[MaintenanceUpdateRequest] = Json.writes[MaintenanceUpdateRequest]

  // Combine Reads and Writes into Format
  implicit val maintenanceUpdateRequestFormat: Format[MaintenanceUpdateRequest] = Format(maintenanceUpdateRequestReads, maintenanceUpdateRequestWrites)
}





