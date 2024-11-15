package repositories

import models.ListEventsRequest
import models.entity.Event

import javax.inject.{Inject, Singleton}
import play.api.db.slick.DatabaseConfigProvider
import slick.jdbc.JdbcProfile

import java.time.LocalDate
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class EventRepository @Inject()(dbConfigProvider: DatabaseConfigProvider)(implicit ec: ExecutionContext) {
  private val dbConfig = dbConfigProvider.get[JdbcProfile]

  import dbConfig._
  import profile.api._

  private class EventTable(tag: Tag) extends Table[Event](tag, "events") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def eventType = column[String]("event_type")
    def eventName = column[String]("event_name")
    def eventDate = column[LocalDate]("event_date")
    def slotNumber = column[Int]("slot_number")
    def guestCount = column[Long]("guest_count")
    def specialRequirements = column[Option[String]]("special_requirements")
    def eventStatus = column[Option[String]]("event_status")

    def * = (id.?, eventType, eventName, eventDate, slotNumber, guestCount, specialRequirements, eventStatus) <> ((Event.apply _).tupled, Event.unapply)
  }

  private val events = TableQuery[EventTable]

  def create(event: Event): Future[Long] = {
    val insertQueryThenReturnId = events returning events.map(_.id)

    db.run(insertQueryThenReturnId += event)
  }

  def getEventById(eventId: Long): Future[Event] = {
    db.run(events.filter(_.id === eventId).result.head)
  }

  def update(eventId: Long, event: Event): Future[Event] = {
    val updateQuery = events.filter(_.id === eventId)
      .map(ele => (ele.eventType, ele.eventName, ele.eventDate, ele.slotNumber, ele.guestCount, ele.specialRequirements, ele.eventStatus))
      .update((event.eventType, event.eventName, event.eventDate, event.slotNumber, event.guestCount, event.specialRequirements, event.eventStatus))

    db.run(updateQuery).flatMap { _ =>
      getEventById(eventId)
    }
  }

  def listEvents(eventType: Option[String], status: Option[String], eventDate: Option[LocalDate], slotNumber: Option[Int]): Future[Seq[Event]] = {
    val query = events
      .filterOpt(status) { case (event, s) => event.eventStatus === s }
      .filterOpt(eventType) { case (event, s) => event.eventType === s }
      .filterOpt(eventDate) { case (event, s) => event.eventDate === s }
      .filterOpt(slotNumber) { case (event, s) => event.slotNumber === s }

    db.run(query.result)
  }

  def checkEventExists(date: LocalDate, slot: Int): Future[Boolean] = {
    db.run(events.filter(event => event.eventDate === date && event.slotNumber === slot).exists.result)
  }

  def getEventsByDate(date: LocalDate): Future[Seq[Event]] = {
    db.run(events.filter(event => event.eventDate === date).result)
  }
}
