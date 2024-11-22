package models.entity

import models.enums.EquipmentCondition.EquipmentCondition
import models.enums.EquipmentType.EquipmentType
import models.enums.EquipmentCondition
import play.api.libs.functional.syntax._
import play.api.libs.json._

case class Equipment(
    id: Option[Long] = None,
    name: String,
    model: String,
    serialNumber: String,
    equipmentType: EquipmentType,
    equipmentCondition: EquipmentCondition = EquipmentCondition.WORKING,
    isAvailable: Boolean = true
)

object Equipment {
  // Read for the Equipment fields
  private val idReads: Reads[Option[Long]] = (JsPath \ "id").readNullable[Long]
  private val nameReads: Reads[String] = (JsPath \ "name").read[String]
  private val modelReads: Reads[String] = (JsPath \ "model").read[String]
  private val serialNumberReads: Reads[String] = (JsPath \ "serialNumber").read[String]
  private val equipmentTypeReads: Reads[EquipmentType] = (JsPath \ "equipmentType").read[EquipmentType]
  private val equipmentConditionReads: Reads[EquipmentCondition] = (JsPath \ "equipmentCondition").readNullable[EquipmentCondition].map(_.getOrElse(EquipmentCondition.WORKING))
  private val isAvailableReads: Reads[Boolean] = (JsPath \ "isAvailable")
    .readNullable[Boolean]
    .map(_.getOrElse(true)) // Default to true if missing

  // Combine all the reads
  implicit val equipmentReads: Reads[Equipment] = (
    idReads and
      nameReads and
      modelReads and
      serialNumberReads and
      equipmentTypeReads and
      equipmentConditionReads and
      isAvailableReads
    )(Equipment.apply _)

  // Use Json.writes to generate Writes automatically
  implicit val equipmentWrites: Writes[Equipment] = Json.writes[Equipment]

  // Combine Reads and Writes into Format
  implicit val equipmentFormat: Format[Equipment] = Format(equipmentReads, equipmentWrites)
}
