package services

import models.entity.{Allocation, Employee}
import models.request.AllocationApprovalRequest
import repositories.EmployeeRepository
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class EmployeeService @Inject()(
                                 employeeRepository: EmployeeRepository,
                                 allocationService: AllocationService
                               )(implicit executionContext: ExecutionContext) {
  def create(employee: Employee): Future[Long] = employeeRepository.create(employee)

  def getEmployeeById(id: Long): Future[Employee] = {
    employeeRepository.getEmployeeById(id).flatMap {
      case Some(employee) => Future.successful(employee)
      case None => Future.failed(new IllegalStateException(s"Employee $id is inactive"))
    }
  }

  def getAllocationsByEmpId(empId: Long): Future[Seq[Allocation]] = {
    getEmployeeById(empId).flatMap(_ =>
      allocationService.getAllocationsByEmpId(empId)
    )
  }

  def getApprovalRequests(empId: Long): Future[Seq[Allocation]] = {
    getEmployeeById(empId).flatMap(_ =>
      allocationService.getApprovalRequests(empId)
    )
  }

  def updateAllocationApproval(request: AllocationApprovalRequest): Future[Int] = allocationService.updateAllocationApproval(request)
}
