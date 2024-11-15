package models.entity

import play.api.libs.functional.syntax._
import play.api.libs.json._

//sealed trait EquipmentType
//case object LAPTOP extends EquipmentType
//case object TABLET extends EquipmentType
//case object PROJECTOR extends EquipmentType
//case object MOUSE extends EquipmentType
//case object KEYBOARD extends EquipmentType
//case object MONITOR extends EquipmentType
//
//object EquipmentType {
//  // Mapping for reads and writes of EquipmentType
//  implicit val equipmentTypeReads: Reads[EquipmentType] = Reads {
//    case JsString("LAPTOP") => JsSuccess(LAPTOP)
//    case JsString("TABLET") => JsSuccess(TABLET)
//    case JsString("PROJECTOR") => JsSuccess(PROJECTOR)
//    case JsString("MOUSE") => JsSuccess(MOUSE)
//    case JsString("KEYBOARD") => JsSuccess(KEYBOARD)
//    case JsString("MONITOR") => JsSuccess(MONITOR)
//    case _ => JsError("Invalid EquipmentType")
//  }
//
//  implicit val equipmentTypeWrites: Writes[EquipmentType] = Writes {
//    case LAPTOP => JsString("LAPTOP")
//    case TABLET => JsString("TABLET")
//    case PROJECTOR => JsString("PROJECTOR")
//    case MOUSE => JsString("MOUSE")
//    case KEYBOARD => JsString("KEYBOARD")
//    case MONITOR => JsString("MONITOR")
//  }
//
//  implicit val equipmentTypeFormat: Format[EquipmentType] = Format(equipmentTypeReads, equipmentTypeWrites)
//}
//
//sealed trait EquipmentCondition
//case object IN_USAGE extends EquipmentCondition
//case object AVAILABLE extends EquipmentCondition
//case object UNDER_MAINTENANCE extends EquipmentCondition
//case object DAMAGED extends EquipmentCondition
//
//object EquipmentCondition {
//  // Mapping for reads and writes of EquipmentCondition
//  implicit val equipmentConditionReads: Reads[EquipmentCondition] = Reads {
//    case JsString("IN_USAGE") => JsSuccess(IN_USAGE)
//    case JsString("AVAILABLE") => JsSuccess(AVAILABLE)
//    case JsString("UNDER_MAINTENANCE") => JsSuccess(UNDER_MAINTENANCE)
//    case JsString("DAMAGED") => JsSuccess(DAMAGED)
//    case _ => JsError("Invalid EquipmentCondition")
//  }
//
//  implicit val equipmentConditionWrites: Writes[EquipmentCondition] = Writes {
//    case IN_USAGE => JsString("IN_USAGE")
//    case AVAILABLE => JsString("AVAILABLE")
//    case UNDER_MAINTENANCE => JsString("UNDER_MAINTENANCE")
//    case DAMAGED => JsString("DAMAGED")
//  }
//
//  implicit val equipmentConditionFormat: Format[EquipmentCondition] = Format(equipmentConditionReads, equipmentConditionWrites)
//}

//// EquipmentType Enumeration
//sealed trait EquipmentType
//object EquipmentType {
//  case object LAPTOP extends EquipmentType
//  case object TABLET extends EquipmentType
//  case object PROJECTOR extends EquipmentType
//  case object MOUSE extends EquipmentType
//  case object KEYBOARD extends EquipmentType
//  case object MONITOR extends EquipmentType
//
//  // JSON serialization for EquipmentType
//  implicit val equipmentTypeFormat: Format[EquipmentType] = new Format[EquipmentType] {
//    override def reads(json: JsValue): JsResult[EquipmentType] = json.as[String] match {
//      case "LAPTOP" => JsSuccess(LAPTOP)
//      case "TABLET" => JsSuccess(TABLET)
//      case "PROJECTOR" => JsSuccess(PROJECTOR)
//      case "MOUSE" => JsSuccess(MOUSE)
//      case "KEYBOARD" => JsSuccess(KEYBOARD)
//      case "MONITOR" => JsSuccess(MONITOR)
//      case other => JsError(s"Unknown equipment type: $other")
//    }
//
//    override def writes(equipmentType: EquipmentType): JsValue = JsString(equipmentType.toString)
//  }
//}
//
//// EquipmentCondition Enumeration
//sealed trait EquipmentCondition
//object EquipmentCondition {
//  case object IN_USAGE extends EquipmentCondition
//  case object AVAILABLE extends EquipmentCondition
//  case object UNDER_MAINTENANCE extends EquipmentCondition
//  case object DAMAGED extends EquipmentCondition
//
//  // JSON serialization for EquipmentCondition
//  implicit val equipmentConditionFormat: Format[EquipmentCondition] = new Format[EquipmentCondition] {
//    override def reads(json: JsValue): JsResult[EquipmentCondition] = json.as[String] match {
//      case "IN_USAGE" => JsSuccess(IN_USAGE)
//      case "AVAILABLE" => JsSuccess(AVAILABLE)
//      case "UNDER_MAINTENANCE" => JsSuccess(UNDER_MAINTENANCE)
//      case "DAMAGED" => JsSuccess(DAMAGED)
//      case other => JsError(s"Unknown equipment condition: $other")
//    }
//
//    override def writes(equipmentCondition: EquipmentCondition): JsValue = JsString(equipmentCondition.toString)
//  }
//}

case class Equipment(
    id: Option[Long] = None,
    name: String,
    model: String,
    serialNumber: String,
    equipmentType: String,
    equipmentCondition: String = "WORKING",
    isAvailable: Boolean = true
)

object Equipment {
  // Read for the Equipment fields
  private val idReads: Reads[Option[Long]] = (JsPath \ "id").readNullable[Long]
  private val nameReads: Reads[String] = (JsPath \ "name").read[String]
  private val modelReads: Reads[String] = (JsPath \ "model").read[String]
  private val serialNumberReads: Reads[String] = (JsPath \ "serialNumber").read[String]
  private val equipmentTypeReads: Reads[String] = (JsPath \ "equipmentType").read[String]
  private val equipmentConditionReads: Reads[String] = (JsPath \ "equipmentCondition").readNullable[String].map(_.getOrElse("WORKING"))
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
