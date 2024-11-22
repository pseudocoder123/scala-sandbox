package services

import models.entity.{Equipment, Maintenance}
import models.enums.EquipmentCondition
import models.request.MaintenanceUpdateRequest
import repositories.MaintenanceRepository
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class MaintenanceService @Inject() (
    maintenanceRepository: MaintenanceRepository,
    equipmentService: EquipmentService,
    kafkaProducerFactory: KafkaProducerFactory
)(implicit executionContext: ExecutionContext) {
  def createMaintenanceRecord(maintenance: Maintenance): Future[Long] = maintenanceRepository.create(maintenance)

  def updateMaintenanceStatus(req: MaintenanceUpdateRequest): Future[Equipment] = {
    maintenanceRepository.update(req.maintenanceId, req.isWorking).flatMap { maintenance => {
      val equipmentStatus = if (req.isWorking) EquipmentCondition.WORKING else EquipmentCondition.DAMAGED
      equipmentService.updateEquipmentCondition(maintenance.equipmentId, equipmentStatus, req.isWorking).map { equipment =>

        kafkaProducerFactory.sendInventoryUpdateAfterMaintenance(equipment, equipmentStatus, maintenance)
        equipment
      }
    }
    }
  }
}
