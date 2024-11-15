package services

import models.entity.{Allocation, Employee}
import models.{AllocationApprovalRequest, AllocationRequest}
import repositories.EmployeeRepository

import javax.inject.{Inject, Singleton}
import scala.concurrent.Future

@Singleton
class EmployeeService @Inject()(
                                 employeeRepository: EmployeeRepository,
                                 allocationService: AllocationService
                               ) {
  def create(employee: Employee): Future[Long] = employeeRepository.create(employee)

  def getEmployeeById(id: Long): Future[Employee] = employeeRepository.getEmployeeById(id)

  def getAllocationsByEmpId(empId: Long): Future[Seq[Allocation]] = allocationService.getAllocationsByEmpId(empId)

  def getApprovalRequests(empId: Long): Future[Seq[Allocation]] = allocationService.getApprovalRequests(empId)

  def updateAllocationApproval(request: AllocationApprovalRequest): Future[Int] = allocationService.updateAllocationApproval(request)
}
