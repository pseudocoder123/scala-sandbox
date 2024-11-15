package repositories

import models.entity.Issue

import javax.inject.{Inject, Singleton}
import play.api.db.slick.DatabaseConfigProvider
import slick.jdbc.JdbcProfile

import java.time.LocalDateTime
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class IssueRepository @Inject()(dbConfigProvider: DatabaseConfigProvider)(implicit ec: ExecutionContext) {
  private val dbConfig = dbConfigProvider.get[JdbcProfile]

  import dbConfig._
  import profile.api._

  private class IssueTable(tag: Tag) extends Table[Issue](tag, "issues") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def taskId = column[Long]("task_id")
    def eventId = column[Long]("event_id")
    def teamId = column[Long]("team_id")
    def issueType = column[String]("issue_type")
    def issueDescription = column[String]("issue_description")
    def reportedAt = column[String]("reported_at")
    def resolvedAt = column[Option[String]]("resolved_at")

    def * = (id.?, taskId, eventId, teamId, issueType, issueDescription, reportedAt, resolvedAt) <> ((Issue.apply _).tupled, Issue.unapply)
  }

  private val issues = TableQuery[IssueTable]

  def create(issue: Issue): Future[Issue] = {
    val insertQueryThenReturnId = issues returning issues.map(_.id)

    db.run(insertQueryThenReturnId += issue).flatMap(
      id => get(id))
  }

  def get(issueId: Long): Future[Issue] = db.run(issues.filter(_.id === issueId).result.head)
}
