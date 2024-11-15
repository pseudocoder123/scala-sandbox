package models.entity

import play.api.libs.functional.syntax._
import play.api.libs.json._

import java.time.LocalDateTime

// Take task Id here also
case class Issue(
                  id: Option[Long] = None,
                  taskId: Long,
                  eventId: Long,
                  teamId: Long,
                  issueType: String,
                  issueDescription: String,
                  reportedAt: String,
                  resolvedAt: Option[String] = None
)

object Issue {
  private val idReads: Reads[Option[Long]] = (JsPath \ "id").readNullable[Long]
  private val taskIdReads: Reads[Long] = (JsPath \ "taskId").read[Long]
  private val eventIdReads: Reads[Long] = (JsPath \ "eventId").read[Long]
  private val teamIdReads: Reads[Long] = (JsPath \ "teamId").read[Long]
  private val issueTypeReads: Reads[String] = (JsPath \ "issueType").read[String]
  private val issueDescriptionReads: Reads[String] = (JsPath \ "issueDescription").read[String]
  private val reportedAtReads: Reads[String] = (JsPath \ "reportedAt").readNullable[String].map(_.getOrElse(LocalDateTime.now().toString))
  private val resolvedAtReads: Reads[Option[String]] = (JsPath \ "resolvedAt").readNullable[String]

  // Combine all the reads
  implicit val issueReads: Reads[Issue] = (
    idReads and
      taskIdReads and
      eventIdReads and
      teamIdReads and
      issueTypeReads and
      issueDescriptionReads and
      reportedAtReads and
      resolvedAtReads
    )(Issue.apply _)

  // Use Json.writes to generate Writes automatically
  implicit val issueWrites: Writes[Issue] = Json.writes[Issue]

  // Combine Reads and Writes into Format
  implicit val issueFormat: Format[Issue] = Format(issueReads, issueWrites)
}
