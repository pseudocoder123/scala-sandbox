package repositories

import models.enums.MaintenanceStatus.MaintenanceStatus
import javax.inject.{Inject, Singleton}
import play.api.db.slick.DatabaseConfigProvider
import slick.jdbc.JdbcProfile
import java.time.LocalDate
import ColumnMappings._
import models.entity.Maintenance
import models.enums.MaintenanceStatus

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class MaintenanceRepository @Inject()(dbConfigProvider: DatabaseConfigProvider)(implicit ec: ExecutionContext){
  private val dbConfig = dbConfigProvider.get[JdbcProfile]

  import dbConfig._
  import profile.api._
  private class MaintenanceTable(tag: Tag) extends Table[Maintenance](tag, "maintenance")  {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def equipmentId = column[Long]("equipment_id")
    def allocationId = column[Option[Long]]("allocation_id")
    def reportedDate = column[LocalDate]("reported_date")
    def status = column[MaintenanceStatus]("status")
    def serviceCompletedOn = column[Option[LocalDate]]("service_completed_on")
    def reportedBy = column[Option[Long]]("reported_by")

    def * = (id.?, equipmentId, allocationId, reportedDate, status, serviceCompletedOn, reportedBy) <> ((Maintenance.apply _).tupled, Maintenance.unapply)
  }

  private val maintenanceRecords = TableQuery[MaintenanceTable]

  def create(maintenance: Maintenance): Future[Long] = {
    val insertQueryThenReturnId = maintenanceRecords returning maintenanceRecords.map(_.id)

    db.run(insertQueryThenReturnId += maintenance)
  }

  def get(maintenanceId: Long): Future[Maintenance] = {
    db.run(maintenanceRecords.filter(_.id === maintenanceId).result.head)
  }

  def update(maintenanceId: Long, isWorking: Boolean): Future[Maintenance] = {
    val maintenanceStatus = if(isWorking) MaintenanceStatus.WORKING else MaintenanceStatus.DAMAGED
    val maintenanceRecord = maintenanceRecords.filter(_.id === maintenanceId)
      .map(ele => (ele.serviceCompletedOn, ele.status))
      .update((Some(LocalDate.now()), maintenanceStatus))

    db.run(maintenanceRecord).flatMap {_ =>
      get(maintenanceId)
    }
  }

}
