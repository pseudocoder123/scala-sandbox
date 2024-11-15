package services

import models.ListEventsRequest
import models.entity.{Event, Task}
import repositories.EventRepository

import java.time.LocalDate
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class EventService @Inject() (
                              eventRepository: EventRepository,
                              taskService: TaskService
                            )(implicit ex:ExecutionContext) {
  def create(event: Event): Future[Long] = {
    eventRepository.checkEventExists(event.eventDate, event.slotNumber).flatMap {value =>{
      val eventStatus: String = if(value) "REJECTED" else "SCHEDULED"
      val updatedEvent = event.copy(eventStatus = Some(eventStatus))
      eventRepository.create(updatedEvent)
    }
    }
  }

  def getEventById(eventId: Long): Future[Event] = eventRepository.getEventById(eventId)

  def update(eventId: Long, event: Event): Future[Event] = eventRepository.update(eventId, event)

  def list(eventType: Option[String], status: Option[String], eventDate: Option[LocalDate], slotNumber: Option[Int])
  : Future[Seq[Event]] = eventRepository.listEvents(eventType: Option[String], status: Option[String], eventDate: Option[LocalDate], slotNumber: Option[Int])

  def getTasksForEventId(eventId: Long): Future[Seq[Task]] = taskService.getTasksForEventId(eventId)
}
