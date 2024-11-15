package services

import models.{AllocationApprovalRequest, AllocationRequest, AllocationResponse, ReturnEquipment, ReturnEquipmentResponse}
import models.entity.{Allocation, Employee, Equipment, Maintenance}
import repositories.{AllocationRepository, EmployeeRepository, EquipmentRepository}
import java.time.LocalDate
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class AllocationService @Inject()(
     allocationRepository: AllocationRepository,
     employeeRepository: EmployeeRepository,
     equipmentService: EquipmentService,
     maintenanceService: MaintenanceService,
     kafkaProducerFactory: KafkaProducerFactory
)(implicit ec: ExecutionContext) {
  // request allocation
  def requestEquipment(allocationRequest: AllocationRequest): Future[AllocationResponse] = {
    employeeRepository.getEmployeeById(allocationRequest.employeeId).flatMap{ employee => {
      val (managerId, approvalRequired) = (employee.managerId, employee.managerId.isDefined)
      allocationRepository.createAllocation(allocationRequest, allocationRequest.createdBy, managerId, approvalRequired).map {allocation =>
        val allocationResponse = AllocationResponse(
          allocationId = allocation.id.get,
          employeeId = allocation.employeeId,
          approvalRequired = approvalRequired,
          equipmentType = allocation.equipmentType,
          allocationStatus = allocation.allocationStatus
        )

        // SEND NOTIFICATION TO MANAGER FOR APPROVAL
        if(approvalRequired) kafkaProducerFactory.sendAllocationApprovalRequest(allocation)

        allocationResponse
      }
    }}
  }

  // Get Allocation details by Allocation Id
  def getAllocationDetails(allocationId: Long): Future[Allocation] =
    allocationRepository.getAllocationDetailsById(allocationId)

  // Get Allocations of Employee Id
  def getAllocationsByEmpId(employeeId: Long): Future[Seq[Allocation]] =
    allocationRepository.getAllocationsByEmpId(employeeId)

  // Get Approval Requests
  def getApprovalRequests(empId: Long): Future[Seq[Allocation]] =
    allocationRepository.getApprovalRequests(empId)

  // Update Allocation Approval
  def updateAllocationApproval(request: AllocationApprovalRequest): Future[Int] = {
    val allocationStatus = if(request.isApproved) "REQUESTED" else "REJECTED"
    allocationRepository.updateAllocationStatus(request.allocationId, allocationStatus)
  }

  // Process Allocation
  def processAllocation(allocationId: Long): Future[AllocationResponse] = {
    allocationRepository.getAllocationDetailsById(allocationId).flatMap { allocation =>
      equipmentService.getEquipmentsByType(allocation.equipmentType).flatMap { equipments =>
        if (equipments.nonEmpty) {
          val equipment = equipments.head
          val updatedAllocation = allocation.copy(
            equipmentId = equipment.id,
            allocationStatus = "ALLOCATED",
            expectedReturnDate = Some(allocation.requestDate.plusDays(20)) // Fixed LocalDate calculation
          )

          // Chain Futures with flatMap
          allocationRepository.updateAllocation(allocationId, updatedAllocation).flatMap { _ =>
            equipmentService.update(equipment.id.get, equipment.copy(isAvailable = false)).map { _ =>
              // Return AllocationResponse after both operations are complete
              val allocationResponse = AllocationResponse(
                allocationId = allocation.id.get,
                employeeId = allocation.employeeId,
                approvalRequired = allocation.managerId.isDefined,
                equipmentType = allocation.equipmentType,
                equipmentId = updatedAllocation.equipmentId,
                expectedReturnDate = updatedAllocation.expectedReturnDate,
                allocationStatus = updatedAllocation.allocationStatus
              )

              // SEND KAFKA NOTIFICATION TO THE INVENTORY MANAGEMENT
              kafkaProducerFactory.sendInventoryUpdateMessage(updatedAllocation, "ALLOCATED")

              allocationResponse
            }
          }

        } else {
          val allocationStatus = "UNAVAILABLE"

          // Return a Future after updating allocation status
          allocationRepository.updateAllocationStatus(allocationId, allocationStatus).map { _ =>
            AllocationResponse(
              allocationId = allocation.id.get,
              employeeId = allocation.employeeId,
              approvalRequired = allocation.managerId.isDefined,
              equipmentType = allocation.equipmentType,
              allocationStatus = allocationStatus
            )
          }
        }
      }
    }
  }

  // Return Equipment
  def returnEquipment(req: ReturnEquipment): Future[ReturnEquipmentResponse] = {
    allocationRepository.getAllocationDetailsById(req.allocationId).flatMap{allocation => {
      val allocationStatus = if(LocalDate.now().isAfter(allocation.expectedReturnDate.get)) "OVERDUE_RETURNED" else "RETURNED"
      val updatedAllocation = allocation.copy(allocationStatus=allocationStatus, returnDate = Some(LocalDate.now()))
      allocationRepository.updateAllocation(allocation.id.get, updatedAllocation).flatMap {_ => {
        equipmentService.getEquipmentById(allocation.equipmentId.get).flatMap{equipment => {
          val equipmentCondition = if(req.maintenanceRequired) "UNDER_MAINTENANCE" else "WORKING"
          val isWorking = !req.maintenanceRequired
          val updatedEquipment = equipment.copy(equipmentCondition=equipmentCondition, isAvailable = isWorking)
          equipmentService.update(updatedEquipment.id.get, updatedEquipment).map{_ =>{
            if(isWorking) {
              // SEND NOTIFICATION TO INVENTORY TEAM
              kafkaProducerFactory.sendInventoryUpdateMessage(updatedAllocation, "RETURNED")
            }else{
              val reportedDate = LocalDate.now()
              maintenanceService.createMaintenanceRecord(
                Maintenance(
                  equipmentId = allocation.equipmentId.get,
                  allocationId = allocation.id,
                  reportedDate = reportedDate,
                  maintenanceStatus = req.maintenanceType.get
                )
              )
              // SEND NOTIFICATION TO MAINTENANCE TEAM
              kafkaProducerFactory.sendMaintenanceNotification(updatedAllocation, reportedDate)
            }

            ReturnEquipmentResponse(allocation.id.get, updatedEquipment.id.get, LocalDate.now())
          }}
        }}
        }
      }}
    }
  }
}