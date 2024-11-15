package models.entity

import play.api.libs.functional.syntax._
import play.api.libs.json._

import java.time.{LocalDate, LocalDateTime}

case class Team(
                  id: Option[Long] = None,
                  teamName: String,
                  teamType: String
                )

object Team {
  private val idReads: Reads[Option[Long]] = (JsPath \ "id").readNullable[Long]
  private val teamNameReads: Reads[String] = (JsPath \ "teamName").read[String]
  private val teamTypeReads: Reads[String] = (JsPath \ "teamType").read[String]

  // Combine all the reads
  implicit val teamReads: Reads[Team] = (
    idReads and
      teamNameReads and
      teamTypeReads
    )(Team.apply _)

  // Use Json.writes to generate Writes automatically
  implicit val teamWrites: Writes[Team] = Json.writes[Team]

  // Combine Reads and Writes into Format
  implicit val teamFormat: Format[Team] = Format(teamReads, teamWrites)
}
