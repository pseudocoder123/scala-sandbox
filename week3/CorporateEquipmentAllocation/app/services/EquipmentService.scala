package services

import models.{AllocationRequest, AllocationResponse}
import models.entity.{Allocation, Employee, Equipment}
import repositories.{AllocationRepository, EquipmentRepository}

import java.time.LocalDate
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class EquipmentService @Inject()(equipmentRepository: EquipmentRepository) {
  // create equipment
  def create(equipment: Equipment): Future[Long] = equipmentRepository.createEquipmentRecord(equipment)

  // update equipment
  def update(id: Long, equipment: Equipment): Future[Equipment] =
    equipmentRepository.update(id, equipment)

  // update equipment status
  def updateStatus(id: Long, status: String): Future[Equipment] =
    equipmentRepository.updateStatus(id, status)

  // get equipment by id
  def getEquipmentById(id: Long): Future[Equipment] = equipmentRepository.getEquipmentById(id)

  // list working equipments of specific equipmentType
  def getEquipmentsByType(equipmentType: String): Future[Seq[Equipment]] = equipmentRepository.getEquipmentsByType(equipmentType)
}