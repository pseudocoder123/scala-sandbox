package services

import models.entity.Equipment
import models.enums.EquipmentCondition.EquipmentCondition
import models.enums.EquipmentType.EquipmentType
import repositories.EquipmentRepository

import java.time.LocalDate
import javax.inject.{Inject, Singleton}
import scala.concurrent.Future

@Singleton
class EquipmentService @Inject()(equipmentRepository: EquipmentRepository) {
  // create equipment
  def create(equipment: Equipment): Future[Long] = {
    equipmentRepository.createEquipmentRecord(equipment)
  }

  // update equipment
  def update(id: Long, equipment: Equipment): Future[Equipment] =
    equipmentRepository.update(id, equipment)

  // update equipment status
  def updateEquipmentCondition(id: Long, status: EquipmentCondition, isAvailable: Boolean): Future[Equipment] =
    equipmentRepository.updateEquipmentCondition(id, status, isAvailable)

  // get equipment by id
  def getEquipmentById(id: Long): Future[Equipment] = equipmentRepository.getEquipmentById(id)

  // list working equipments of specific equipmentType
  def getEquipmentsByType(equipmentType: EquipmentType): Future[Seq[Equipment]] = equipmentRepository.getEquipmentsByType(equipmentType)
}