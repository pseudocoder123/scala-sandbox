package repositories

import models.AllocationRequest
import models.entity.Allocation

import javax.inject.{Inject, Singleton}
import play.api.db.slick.DatabaseConfigProvider
import slick.jdbc.JdbcProfile

import java.time.LocalDate
import java.util.Date
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class AllocationRepository @Inject()(dbConfigProvider: DatabaseConfigProvider)(implicit ec: ExecutionContext){
  private val dbConfig = dbConfigProvider.get[JdbcProfile]

  import dbConfig._
  import profile.api._

//  // Implicit mapping for java.util.Date <-> java.sql.Date
//  private object AllocationTable {
//    implicit val utilDateColumnType: BaseColumnType[Date] = MappedColumnType.base[Date, java.sql.Date](
//      utilDate => new java.sql.Date(utilDate.getTime),
//      sqlDate => new Date(sqlDate.getTime)
//    )
//  }

  private class AllocationTable(tag: Tag) extends Table[Allocation](tag, "allocations")  {
//    import AllocationTable.utilDateColumnType

    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def employeeId = column[Long]("employee_id")
    def managerId = column[Option[Long]]("manager_id")
    def equipmentType = column[String]("equipment_type")
    def equipmentId = column[Option[Long]]("equipment_id")
    def allocationStatus = column[String]("allocation_status")
    def purpose = column[Option[String]]("purpose")
    def requestDate = column[LocalDate]("request_date")
    def expectedReturnDate = column[Option[LocalDate]]("expected_return_date")
    def returnDate = column[Option[LocalDate]]("return_date")
    def createdBy = column[String]("created_by")

    def * = (id.?, employeeId, managerId, equipmentType, equipmentId, allocationStatus, purpose, requestDate, expectedReturnDate, returnDate, createdBy) <> ((Allocation.apply _).tupled, Allocation.unapply)
  }

  private val allocations = TableQuery[AllocationTable]

  def createAllocation(allocationRequest: AllocationRequest,
                       requestedBy: String,
                       managerId: Option[Long],
                       approvalRequired: Boolean): Future[Allocation] = {
    val insertQueryThenReturnId = allocations returning allocations.map(_.id) into ((allocation, id) => allocation.copy(id = Some(id)))
    val allocation = Allocation(
      employeeId = allocationRequest.employeeId,
      managerId = managerId,
      equipmentType = allocationRequest.equipmentType,
      allocationStatus = if(approvalRequired) "APPROVAL_PENDING" else "REQUESTED",
      purpose = allocationRequest.purpose,
      requestDate = LocalDate.now(),
      createdBy = requestedBy
    )
    db.run(insertQueryThenReturnId += allocation)
  }

  def getAllocationDetailsById(allocationId: Long): Future[Allocation] = {
    db.run(allocations.filter(allocation => allocation.id === allocationId).result.head)
  }

  def getAllocationsByEmpId(employeeId: Long): Future[Seq[Allocation]] = {
    db.run(allocations.filter(allocation => allocation.employeeId === employeeId).result)
  }

  def getApprovalRequests(empId: Long): Future[Seq[Allocation]] = {
    db.run(allocations.filter(allocation => allocation.managerId === empId).result)
  }

  def updateAllocationStatus(allocationId: Long, allocationStatus: String): Future[Int] = {
    val updateQuery = allocations.filter(allocation => allocation.id === allocationId)
      .map(ele => ele.allocationStatus)
      .update(allocationStatus)

    db.run(updateQuery)
  }

  def updateAllocation(id: Long, allocation: Allocation): Future[Allocation] = {
    val updateQuery = allocations.filter(allocation => allocation.id === id)
      .map(ele => (ele.employeeId, ele.managerId, ele.equipmentType, ele.equipmentId, ele.allocationStatus, ele.purpose,
        ele.requestDate, ele.expectedReturnDate, ele.returnDate, ele.createdBy))
      .update((allocation.employeeId, allocation.managerId, allocation.equipmentType, allocation.equipmentId, allocation.allocationStatus, allocation.purpose,
        allocation.requestDate, allocation.expectedReturnDate, allocation.returnDate, allocation.createdBy))

    // flatMap removes Some
    db.run(updateQuery).flatMap { _ =>
      getAllocationDetailsById(id)
    }
  }

  // Get the overdue allocation details
  def getOverdueAllocationDetails(currentDate: LocalDate): Future[Seq[Allocation]] = {
    val query = allocations.filter(ele => ele.expectedReturnDate.isDefined &&
      ele.expectedReturnDate.get < currentDate && ele.returnDate.isEmpty)

    // Adding error handling
    db.run(query.result)
  }

}
