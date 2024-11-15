package repositories

import models.entity.Notifications

import javax.inject.{Inject, Singleton}
import play.api.db.slick.DatabaseConfigProvider
import slick.jdbc.JdbcProfile

import java.time.LocalDate
import java.util.Date
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class NotificationsRepository @Inject()(dbConfigProvider: DatabaseConfigProvider)(implicit ec: ExecutionContext){
  private val dbConfig = dbConfigProvider.get[JdbcProfile]

  import dbConfig._
  import profile.api._

//  private object NotificationsTable {
//    implicit val utilDateColumnType: BaseColumnType[Date] = MappedColumnType.base[Date, java.sql.Date](
//      utilDate => new java.sql.Date(utilDate.getTime),
//      sqlDate => new Date(sqlDate.getTime)
//    )
//  }

  private class NotificationsTable(tag: Tag) extends Table[Notifications](tag, "notifications")  {
//    import NotificationsTable.utilDateColumnType

    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def notificationType = column[String]("notification_type")
    def allocationId = column[Long]("allocation_id")
    def maintenanceId = column[Option[Long]]("maintenance_id")
    def createdOn = column[LocalDate]("created_on")

    def * = (id.?, notificationType, allocationId, maintenanceId, createdOn) <> ((Notifications.apply _).tupled, Notifications.unapply)
  }

  private val allocations = TableQuery[NotificationsTable]

}
