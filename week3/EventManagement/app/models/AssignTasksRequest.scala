package models

import play.api.libs.functional.syntax._
import play.api.libs.json._

import java.time.{LocalDate, LocalDateTime}

case class TasksRequest(teamId: Long, taskDescription: String,
                        deadLine: String, specialInstructions: Option[String])

object TasksRequest {
  // Reads for TasksRequest model
  implicit val tasksRequestReads: Reads[TasksRequest] = (
    (JsPath \ "teamId").read[Long] and
      (JsPath \ "taskDescription").read[String] and
      (JsPath \ "deadLine").read[String] and
      (JsPath \ "specialInstructions").readNullable[String]
    )(TasksRequest.apply _)

  // Writes to automatically serialize the TasksRequest to JSON
  implicit val tasksRequestWrites: Writes[TasksRequest] = Json.writes[TasksRequest]

  // Combine Reads and Writes into Format for automatic handling
  implicit val tasksRequestFormat: Format[TasksRequest] = Format(tasksRequestReads, tasksRequestWrites)
}

case class AssignTasksRequest(
                              eventId: Long,
                              tasks: Seq[TasksRequest]
                            )

object AssignTasksRequest {
  private val eventIdReads: Reads[Long] = (JsPath \ "eventId").read[Long]
  private val tasksReads: Reads[Seq[TasksRequest]] = (JsPath \ "tasks").read[Seq[TasksRequest]]

  // Combine all the reads
  implicit val assignTasksRequestReads: Reads[AssignTasksRequest] = (
    eventIdReads and
      tasksReads
    )(AssignTasksRequest.apply _)

  // Use Json.writes to generate Writes automatically
  implicit val assignTasksRequestWrites: Writes[AssignTasksRequest] = Json.writes[AssignTasksRequest]

  // Combine Reads and Writes into Format
  implicit val assignTasksRequestFormat: Format[AssignTasksRequest] = Format(assignTasksRequestReads, assignTasksRequestWrites)
}
