package repositories

import models.entity.User
import play.api.db.slick.DatabaseConfigProvider
import slick.jdbc.JdbcProfile

import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class UserRepository @Inject()(dbConfigProvider: DatabaseConfigProvider)(implicit ec: ExecutionContext){
  private val dbConfig = dbConfigProvider.get[JdbcProfile]

  import dbConfig._
  import profile.api._

  private class UserTable(tag: Tag) extends Table[User](tag, "users")  {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def project = column[String]("project")
    def name = column[String]("name")
    def password = column[String]("password")
    def email = column[String]("email")

    def * = (id.?, project, name, password, email) <> ((User.apply _).tupled, User.unapply)
  }

  private val users = TableQuery[UserTable]

  def create(user: User): Future[Long] = {
    val insertQueryThenReturnId = users returning users.map(_.id)

    db.run(insertQueryThenReturnId += user)
  }

  def getUserByName(name: String): Future[Option[User]] = {
    db.run(users.filter(user => user.name === name && user.project === "EVENT_MANAGEMENT").result.headOption)
  }
}
