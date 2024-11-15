package models.entity

import play.api.libs.functional.syntax._
import play.api.libs.json._

import java.time.LocalDate
import java.util.Date

case class Allocation(
     id: Option[Long] = None,
     employeeId: Long,
     managerId: Option[Long] = None,
     equipmentType: String,
     equipmentId: Option[Long] = None,
     allocationStatus: String,
     purpose: Option[String] = None,
     requestDate: LocalDate,
     expectedReturnDate: Option[LocalDate] = None,
     returnDate: Option[LocalDate] = None,
     createdBy: String
)

object Allocation {
  // Read for the Allocation fields
  private val idReads: Reads[Option[Long]] = (JsPath \ "id").readNullable[Long]
  private val employeeIdReads: Reads[Long] = (JsPath \ "name").read[Long]
  private val managerIdReads: Reads[Option[Long]] = (JsPath \ "department").readNullable[Long]
  private val equipmentTypeReads: Reads[String] = (JsPath \ "designation").read[String]
  private val equipmentIdReads: Reads[Option[Long]] = (JsPath \ "email").readNullable[Long]
  private val allocationStatusReads: Reads[String] = (JsPath \ "managerId").read[String]
  private val purposeReads: Reads[Option[String]] = (JsPath \ "purpose").readNullable[String]
  private val requestDateReads: Reads[LocalDate] = (JsPath \ "requestDate").read[LocalDate]
  private val expectedReturnDateReads: Reads[Option[LocalDate]] = (JsPath \ "expectedReturnDate").readNullable[LocalDate]
  private val returnDateReads: Reads[Option[LocalDate]] = (JsPath \ "returnDate").readNullable[LocalDate]
  private val createdByReads: Reads[String] = (JsPath \ "createdBy").read[String]

  // Combine all the reads
  implicit val allocationReads: Reads[Allocation] = (
    idReads and
      employeeIdReads and
      managerIdReads and
      equipmentTypeReads and
      equipmentIdReads and
      allocationStatusReads and
      purposeReads and
      requestDateReads and
      expectedReturnDateReads and
      returnDateReads and
      createdByReads
    )(Allocation.apply _)

  // Use Json.writes to generate Writes automatically
  implicit val allocationWrites: Writes[Allocation] = Json.writes[Allocation]

  // Combine Reads and Writes into Format
  implicit val allocationFormat: Format[Allocation] = Format(allocationReads, allocationWrites)
}


