package models.entity

import play.api.libs.functional.syntax._
import play.api.libs.json._

case class User(
                     id: Option[Long] = None,
                     project: String = "EVENT_MANAGEMENT",
                     name: String,
                     password: String,
                     email: String
                   )

object User {
  private val idReads: Reads[Option[Long]] = (JsPath \ "id").readNullable[Long]
  private val projectReads: Reads[String] = (JsPath \ "project")
    .readNullable[String].map(_.getOrElse("EVENT_MANAGEMENT"))
  private val nameReads: Reads[String] = (JsPath \ "name").read[String]
  private val passwordReads: Reads[String] = (JsPath \ "password").read[String]
  private val emailReads: Reads[String] = (JsPath \ "email").read[String]

  // Combine all the reads
  implicit val userReads: Reads[User] = (
    idReads and
      projectReads and
      nameReads and
      passwordReads and
      emailReads
    )(User.apply _)

  // Use Json.writes to generate Writes automatically
  implicit val userWrites: Writes[User] = Json.writes[User]

  // Combine Reads and Writes into Format
  implicit val userFormat: Format[User] = Format(userReads, userWrites)
}
