package repositories

import models.enums.EquipmentCondition.EquipmentCondition
import models.enums.EquipmentType.EquipmentType
import javax.inject.{Inject, Singleton}
import play.api.db.slick.DatabaseConfigProvider
import slick.jdbc.JdbcProfile
import slick.lifted.ProvenShape
import ColumnMappings._
import models.entity.Equipment
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class EquipmentRepository @Inject()(dbConfigProvider: DatabaseConfigProvider)(implicit ec: ExecutionContext) {
  private val dbConfig = dbConfigProvider.get[JdbcProfile]
  import dbConfig._
  import profile.api._

  private class EquipmentTable(tag: Tag) extends Table[Equipment](tag, "equipments")  {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def name = column[String]("name")
    def model = column[String]("model")
    def serialNumber = column[String]("serial_number")
    def equipmentType = column[EquipmentType]("equipment_type")
    def equipmentCondition = column[EquipmentCondition]("equipment_condition")
    def isAvailable = column[Boolean]("is_available", O.Default(true))

    def * : ProvenShape[Equipment] = (id.?, name, model, serialNumber, equipmentType, equipmentCondition, isAvailable) <> ((Equipment.apply _).tupled, Equipment.unapply)
  }

  private val equipments = TableQuery[EquipmentTable]

  // Create an Equipment
  def createEquipmentRecord(equipment: Equipment): Future[Long] = {
    val insertQueryThenReturnId = equipments returning equipments.map(_.id)

    db.run(insertQueryThenReturnId += equipment)
  }

  // Update an Equipment
  def update(id: Long, equipment: Equipment): Future[Equipment] = {
    val updateQuery = equipments.filter(equipment => equipment.id === id)
      .map(ele => (ele.name, ele.model, ele.serialNumber, ele.equipmentType, ele.equipmentCondition, ele.isAvailable))
      .update((equipment.name, equipment.model, equipment.serialNumber, equipment.equipmentType, equipment.equipmentCondition, equipment.isAvailable))

    db.run(updateQuery).flatMap {_ =>
      getEquipmentById(id)
    }
  }

  // Update status
  def updateEquipmentCondition(id: Long, status: EquipmentCondition, isAvailable: Boolean): Future[Equipment] = {
    val equipment = equipments.filter(_.id === id)
      .map(ele => (ele.equipmentCondition, ele.isAvailable))
      .update((status, isAvailable))

    db.run(equipment).flatMap{_ =>
      getEquipmentById(id)
    }
  }

  // Get Equipment details by id
  def getEquipmentById(id: Long): Future[Equipment] =
    db.run(equipments.filter(equipment => equipment.id === id).result.head)

  // Get Available Equipments by type
  def getEquipmentsByType(equipmentType: EquipmentType): Future[Seq[Equipment]] =
    db.run(equipments.filter(eq => eq.equipmentType === equipmentType && eq.isAvailable).result)
}
