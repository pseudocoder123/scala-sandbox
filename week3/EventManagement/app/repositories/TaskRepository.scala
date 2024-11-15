package repositories

import models.ListEventsRequest
import models.entity.{Event, Task}

import javax.inject.{Inject, Singleton}
import play.api.db.slick.DatabaseConfigProvider
import slick.jdbc.JdbcProfile

import java.time.{LocalDate, LocalDateTime}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class TaskRepository @Inject()(dbConfigProvider: DatabaseConfigProvider)(implicit ec: ExecutionContext){
  private val dbConfig = dbConfigProvider.get[JdbcProfile]

  import dbConfig._
  import profile.api._

  private class TaskTable(tag: Tag) extends Table[Task](tag, "tasks")  {
    //    import NotificationsTable.utilDateColumnType

    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def eventId = column[Long]("event_id")
    def teamId = column[Long]("team_id")
    def taskDescription = column[String]("task_description")
    def deadLine = column[String]("deadline")
    def specialInstructions = column[Option[String]]("special_instructions")
    def status = column[String]("status")
    def createdAt = column[String]("created_at")

    def * = (id.?, eventId, teamId, taskDescription, deadLine, specialInstructions, status, createdAt) <> ((Task.apply _).tupled, Task.unapply)
  }

  private val tasks = TableQuery[TaskTable]

  def create(task: Task): Future[Long] = {
    val insertQueryThenReturnId = tasks returning tasks.map(_.id)

    db.run(insertQueryThenReturnId += task)
  }

  def getEventById(taskId: Long): Future[Task] = {
    db.run(tasks.filter(_.id === taskId).result.head)
  }

  def updateStatus(taskId: Long, status: String): Future[Task] = {
    val updateQuery = tasks.filter(_.id === taskId)
      .map(ele => ele.status)
      .update(status)

    db.run(updateQuery).flatMap { _ =>
      getEventById(taskId)
    }
  }

  def getTasksForEventId(eventId: Long): Future[Seq[Task]] = {
    val query = tasks.filter(_.eventId === eventId).result
    db.run(query)
  }

  def assignTasks(tasksList: List[Task]): Future[List[Task]] = {
    val actions = tasksList.map { task =>
      (tasks returning tasks.map(_.id) into ((task, id) => task.copy(id = Some(id)))) += task
    }

    db.run(DBIO.sequence(actions)).map(_.toList)
  }

}