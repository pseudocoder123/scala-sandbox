package repositories

import models.entity.Employee
import javax.inject.{Inject, Singleton}
import play.api.db.slick.DatabaseConfigProvider
import slick.jdbc.JdbcProfile
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class EmployeeRepository @Inject()(dbConfigProvider: DatabaseConfigProvider)(implicit ec: ExecutionContext){
  private val dbConfig = dbConfigProvider.get[JdbcProfile]

  import dbConfig._
  import profile.api._

  private class EmployeeTable(tag: Tag) extends Table[Employee](tag, "employees")  {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def name = column[String]("name")
    def department = column[String]("department")
    def designation = column[String]("designation")
    def email = column[String]("email")
    def managerId = column[Option[Long]]("manager_id")
    def isActive = column[Boolean]("is_active", O.Default(true))

    def * = (id.?, name, department, designation, email, managerId, isActive) <> ((Employee.apply _).tupled, Employee.unapply)
  }

  private val employees = TableQuery[EmployeeTable]

  def create(employee: Employee): Future[Long] = {
    val insertQueryThenReturnId = employees returning employees.map(_.id)

    db.run(insertQueryThenReturnId += employee)
  }

  def getEmployeeById(id: Long): Future[Option[Employee]] = {
    db.run(employees.filter(employee => employee.id === id && employee.isActive === true).result.headOption)
  }

}
