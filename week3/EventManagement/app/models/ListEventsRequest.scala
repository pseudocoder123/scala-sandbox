package models

import play.api.libs.functional.syntax._
import play.api.libs.json._

import java.time.{LocalDate, LocalDateTime}

case class ListEventsRequest(
                  eventType: Option[String] = None,
                  status: Option[String] = None,
                  eventDate: Option[String] = None,
                  slotNumber: Option[Int] = None
)

object ListEventsRequest {
  private val eventTypeReads: Reads[Option[String]] = (JsPath \ "eventType").readNullable[String]
  private val statusReads: Reads[Option[String]] = (JsPath \ "status").readNullable[String]
  private val eventDateReads: Reads[Option[String]] = (JsPath \ "eventDate").readNullable[String]
  private val slotNumberReads: Reads[Option[Int]] = (JsPath \ "slotNumber").readNullable[Int]

  // Combine all the reads
  implicit val issueReads: Reads[ListEventsRequest] = (
    eventTypeReads and
      statusReads and
      eventDateReads and
      slotNumberReads
    )(ListEventsRequest.apply _)

  // Use Json.writes to generate Writes automatically
  implicit val issueWrites: Writes[ListEventsRequest] = Json.writes[ListEventsRequest]

  // Combine Reads and Writes into Format
  implicit val issueFormat: Format[ListEventsRequest] = Format(issueReads, issueWrites)
}
