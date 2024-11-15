package services

import models.entity.{Allocation, Employee, Maintenance}
import models.{AllocationApprovalRequest, AllocationRequest, MaintenanceUpdateRequest}
import repositories.{EmployeeRepository, MaintenanceRepository}

import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class MaintenanceService @Inject() (
                                  maintenanceRepository: MaintenanceRepository,
                                  equipmentService: EquipmentService
                                )(implicit executionContext: ExecutionContext) {
  def createMaintenanceRecord(maintenance: Maintenance): Future[Long] = maintenanceRepository.create(maintenance)

  def updateMaintenanceStatus(req: MaintenanceUpdateRequest): Future[String] = {
    maintenanceRepository.update(req.maintenanceId).flatMap {maintenance =>{
      val equipmentStatus = if(req.isWorking) "WORKING" else "DAMAGED"
      equipmentService.updateStatus(maintenance.equipmentId, equipmentStatus).map{_ =>
        "UPDATED"
      }
    }}
  }
}
