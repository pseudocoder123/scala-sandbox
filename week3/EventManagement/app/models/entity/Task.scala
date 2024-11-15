package models.entity

import play.api.libs.functional.syntax._
import play.api.libs.json._

case class Task(
                  id: Option[Long] = None,
                  eventId: Long,
                  teamId: Long,
                  taskDescription: String,
                  deadLine: String,
                  specialInstructions: Option[String],
                  status: String,
                  createdAt: String
                )

object Task {
  private val idReads: Reads[Option[Long]] = (JsPath \ "id").readNullable[Long]
  private val eventIdReads: Reads[Long] = (JsPath \ "eventId").read[Long]
  private val teamIdReads: Reads[Long] = (JsPath \ "teamId").read[Long]
  private val taskDescriptionReads: Reads[String] = (JsPath \ "taskDescription").read[String]
  private val deadLineReads: Reads[String] = (JsPath \ "deadLine").read[String]
  private val specialInstructionsReads: Reads[Option[String]] = (JsPath \ "specialInstructions").readNullable[String]
  private val statusReads: Reads[String] = (JsPath \ "status").read[String]
  private val createdAtReads: Reads[String] = (JsPath \ "createdAt").read[String]

  // Combine all the reads
  implicit val taskReads: Reads[Task] = (
    idReads and
      eventIdReads and
      teamIdReads and
      taskDescriptionReads and
      deadLineReads and
      specialInstructionsReads and
      statusReads and
      createdAtReads
    )(Task.apply _)

  // Use Json.writes to generate Writes automatically
  implicit val taskWrites: Writes[Task] = Json.writes[Task]

  // Combine Reads and Writes into Format
  implicit val taskFormat: Format[Task] = Format(taskReads, taskWrites)
}
