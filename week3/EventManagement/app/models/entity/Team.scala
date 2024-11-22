package models.entity

import models.enums.TeamType.TeamType
import play.api.libs.functional.syntax._
import play.api.libs.json._

case class Team(
                  id: Option[Long] = None,
                  teamName: String,
                  teamType: TeamType
                )

object Team {
  private val idReads: Reads[Option[Long]] = (JsPath \ "id").readNullable[Long]
  private val teamNameReads: Reads[String] = (JsPath \ "teamName").read[String]
  private val teamTypeReads: Reads[TeamType] = (JsPath \ "teamType").read[TeamType]

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
