package controllers

import java.time.format.DateTimeParseException
import models.entity.Event
import play.api.mvc._
import play.api.libs.json._
import services.{EventService, TaskService}

import java.time.LocalDate
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class EventController @Inject()(
                                val cc: ControllerComponents,
                                eventService: EventService
                              )(implicit ec: ExecutionContext) extends AbstractController(cc) {

  // Create an event
  def createEvent(): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[Event] match {
      case JsSuccess(event, _) =>
        eventService.create(event).map(created =>
          Created(Json.toJson(created)))
      case JsError(errors) =>
        Future.successful(BadRequest(Json.obj(
          "message" -> "Invalid Event data",
          "errors" -> JsError.toJson(errors))))
    }
  }

  // Get event details
  def getEventById(eventId: Long): Action[AnyContent] = Action.async {
    eventService.getEventById(eventId).map(created =>
      Ok(Json.toJson(created)))
  }

  // Update event details
  def updateEvent(eventId: Long): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[Event] match {
      case JsSuccess(event, _) =>
        eventService.update(eventId, event).map(updated =>
          Ok(Json.toJson(updated)))
      case JsError(errors) =>
        Future.successful(BadRequest(Json.obj(
          "message" -> "Invalid Event data",
          "errors" -> JsError.toJson(errors))))
    }
  }

  // Get the list of events
  def listEvents(eventType: Option[String], status: Option[String], eventDate: Option[String], slotNumber: Option[Int])
  : Action[AnyContent] = Action.async {
    val parsedDate: Option[LocalDate] = eventDate.flatMap { date =>
      try {
        Some(LocalDate.parse(date))
      } catch {
        case _: DateTimeParseException => None
      }
    }
    eventService.list(eventType, status, parsedDate, slotNumber).map(response => Ok(Json.toJson(response)))
  }

  // Get tasks for an eventId
  def getTasksForEventId(eventId: Long): Action[AnyContent] = Action.async {
    eventService.getTasksForEventId(eventId).map {created =>
      Ok(Json.toJson(created))
    }
  }
}
