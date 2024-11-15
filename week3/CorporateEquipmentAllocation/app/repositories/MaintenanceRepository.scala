package repositories

import models.entity.Maintenance

import javax.inject.{Inject, Singleton}
import play.api.db.slick.DatabaseConfigProvider
import slick.jdbc.JdbcProfile

import java.time.LocalDate
import java.util.Date
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class MaintenanceRepository @Inject()(dbConfigProvider: DatabaseConfigProvider)(implicit ec: ExecutionContext){
  private val dbConfig = dbConfigProvider.get[JdbcProfile]

  import dbConfig._
  import profile.api._

  // Implicit mapping for java.util.Date <-> java.sql.Date
//  private object MaintenanceTable {
//    implicit val utilDateColumnType: BaseColumnType[Date] = MappedColumnType.base[Date, java.sql.Date](
//      utilDate => new java.sql.Date(utilDate.getTime),
//      sqlDate => new Date(sqlDate.getTime)
//    )
//  }

  private class MaintenanceTable(tag: Tag) extends Table[Maintenance](tag, "maintenance")  {
//    import MaintenanceTable.utilDateColumnType

    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def equipmentId = column[Long]("equipment_id")
    def allocationId = column[Option[Long]]("allocation_id")
    def reportedDate = column[LocalDate]("reported_date")
    def maintenanceStatus = column[String]("maintenance_status")
    def serviceCompletedOn = column[Option[LocalDate]]("service_completed_on")
    def reportedBy = column[Option[Long]]("reported_by")

    def * = (id.?, equipmentId, allocationId, reportedDate, maintenanceStatus, serviceCompletedOn, reportedBy) <> ((Maintenance.apply _).tupled, Maintenance.unapply)
  }

  private val maintenanceRecords = TableQuery[MaintenanceTable]

  def create(maintenance: Maintenance): Future[Long] = {
    val insertQueryThenReturnId = maintenanceRecords returning maintenanceRecords.map(_.id)

    db.run(insertQueryThenReturnId += maintenance)
  }

  def get(maintenanceId: Long): Future[Maintenance] = {
    db.run(maintenanceRecords.filter(_.id === maintenanceId).result.head)
  }

  def update(maintenanceId: Long): Future[Maintenance] = {
    val maintenanceRecord = maintenanceRecords.filter(_.id === maintenanceId)
      .map(ele => ele.serviceCompletedOn)
      .update(Some(LocalDate.now()))

    db.run(maintenanceRecord).flatMap {_ =>
      get(maintenanceId)
    }
  }

}
