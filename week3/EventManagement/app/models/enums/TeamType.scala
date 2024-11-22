package models.enums

import play.api.libs.json.{Format, JsResult, JsString, JsValue}

object TeamType extends Enumeration {
  type TeamType = Value

  val CATERING: Value = Value("CATERING")
  val ENTERTAINMENT: Value = Value("ENTERTAINMENT")
  val DECORATIONS: Value = Value("DECORATIONS")
  val LOGISTICS: Value = Value("LOGISTICS")

  // Implicit Format for TeamType enum
  implicit val teamTypeFormat: Format[TeamType] = new Format[TeamType] {
    def reads(json: JsValue): JsResult[TeamType] = json.validate[String].map(TeamType.withName)
    def writes(status: TeamType): JsValue = JsString(status.toString)
  }

  def withNameOption(name: Option[String]): Option[TeamType] = name match {
    case Some(name) => name.toUpperCase match {
      case "CATERING" => Some(TeamType.CATERING)
      case "ENTERTAINMENT" => Some(TeamType.ENTERTAINMENT)
      case "DECORATIONS" => Some(TeamType.DECORATIONS)
      case "LOGISTICS" => Some(TeamType.LOGISTICS)
      case _ => None
    }
    case _ => None
  }
}