package models.entity

import play.api.libs.functional.syntax._
import play.api.libs.json._

case class Employee(
    id: Option[Long] = None,
    name: String,
    department: String,
    designation: String,
    email: String,
    managerId: Option[Long] = None,
    isActive: Boolean = true
)

object Employee {
  // Read for the Employee fields
  private val idReads: Reads[Option[Long]] = (JsPath \ "id").readNullable[Long]
  private val nameReads: Reads[String] = (JsPath \ "name").read[String]
  private val departmentReads: Reads[String] = (JsPath \ "department").read[String]
  private val designationReads: Reads[String] = (JsPath \ "designation").read[String]
  private val emailReads: Reads[String] = (JsPath \ "email").read[String]
  private val managerIdReads: Reads[Option[Long]] = (JsPath \ "managerId").readNullable[Long]
  private val isActiveReads: Reads[Boolean] = (JsPath \ "isActive")
    .readNullable[Boolean]
    .map(_.getOrElse(true)) // Default to true if missing

  // Combine all the reads
  implicit val employeeReads: Reads[Employee] = (
    idReads and
    nameReads and
    departmentReads and
    designationReads and
    emailReads and
    managerIdReads and
    isActiveReads
    )(Employee.apply _)

  // Use Json.writes to generate Writes automatically
  implicit val employeeWrites: Writes[Employee] = Json.writes[Employee]

  // Combine Reads and Writes into Format
  implicit val employeeFormat: Format[Employee] = Format(employeeReads, employeeWrites)
}
