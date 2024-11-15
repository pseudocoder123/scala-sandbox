package repositories

import models.entity.Notification

import javax.inject.{Inject, Singleton}
import play.api.db.slick.DatabaseConfigProvider
import slick.jdbc.JdbcProfile

import java.time.{LocalDate, LocalDateTime}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class NotificationRepository @Inject()(dbConfigProvider: DatabaseConfigProvider)(implicit ec: ExecutionContext) {
  private val dbConfig = dbConfigProvider.get[JdbcProfile]

  import dbConfig._
  import profile.api._

  private class NotificationTable(tag: Tag) extends Table[Notification](tag, "notifications") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def taskId = column[Long]("task_id")
    def teamId = column[Long]("team_id")
    def notificationType = column[String]("notification_type")
    def sentAt = column[String]("sent_at")

    def * = (id.?, taskId, teamId, notificationType, sentAt) <> ((Notification.apply _).tupled, Notification.unapply)
  }

  private val notifications = TableQuery[NotificationTable]
}
