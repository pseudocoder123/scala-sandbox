package controllers

import java.time.format.DateTimeParseException
import models.entity.Event
import models.enums.EventStatus
import models.response.ApiResponse
import play.api.mvc._
import play.api.libs.json._
import services.EventService
import java.time.LocalDate
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class EventController @Inject()(
                                val cc: ControllerComponents,
                                eventService: EventService
                              )(implicit ec: ExecutionContext) extends AbstractController(cc) {

  /**
   * Creates a new event in the system.
   * Validates and processes the event information provided in the JSON payload.
   *
   * @return An Action wrapper containing the HTTP response:
   *         - 201 (Created) with success message and created event ID
   */
  def createEvent(): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[Event] match {
      case JsSuccess(event, _) =>
        eventService.create(event).map { created =>
          ApiResponse.successResult(201, Json.obj("message" -> "Event created", "id" -> created))
        }.recover {
          case ex: Exception =>
            ApiResponse.errorResult(s"Error creating event: $ex", 400)
        }
      case JsError(errors) =>
        Future.successful(ApiResponse.errorResult(
          "Invalid event request data",
          400
        ))
    }
  }

  /**
   * Retrieves detailed information for a specific event by its ID.
   *
   * @param eventId The unique identifier of the event to retrieve
   * @return An Action wrapper containing the HTTP response:
   *         - 200 (OK) with the event details as JSON
   */
  def getEventById(eventId: Long): Action[AnyContent] = Action.async {
    eventService.getEventById(eventId).map(created =>
      ApiResponse.successResult(200, Json.toJson(created)))
  }

  /**
   * Updates an existing event's details identified by its ID.
   *
   * @return An Action wrapper containing the HTTP response:
   *         - 200 (OK) with the updated event details
   */
  def updateEvent(eventId: Long): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[Event] match {
      case JsSuccess(event, _) =>
        eventService.update(eventId, event).map(updated =>
          ApiResponse.successResult(200, Json.toJson(updated)))
      case JsError(errors) =>
        Future.successful(ApiResponse.errorResult(
          "Invalid updatedEvent request data",
          400
        ))
    }
  }

  /**
   * Retrieves a filtered list of events based on optional search criteria.
   *
   * @param eventType Optional type of event to filter by
   * @param status Optional status to filter by (converted to EventStatus enum)
   * @param eventDate Optional date to filter by (format: yyyy-MM-dd)
   * @param slotNumber Optional slot number to filter by
   * @return An Action wrapper containing the HTTP response:
   *         - 200 (OK) with JSON array of matching events
   */
  def listEvents(eventType: Option[String], status: Option[String], eventDate: Option[String], slotNumber: Option[Int])
  : Action[AnyContent] = Action.async {
    val parsedDate: Option[LocalDate] = eventDate.flatMap { date =>
      try {
        Some(LocalDate.parse(date))
      } catch {
        case _: DateTimeParseException => None
      }
    }
    val statusEnum = EventStatus.withNameOption(status)
    eventService.list(eventType, statusEnum, parsedDate, slotNumber).map(response =>
      ApiResponse.successResult(200, Json.toJson(response))
    )
  }

  /**
   * Retrieves all tasks associated with a specific event.
   *
   * @param eventId The unique identifier of the event
   * @return An Action wrapper containing the HTTP response:
   *         - 200 (OK) with JSON array of associated tasks
   */
  def getTasksForEventId(eventId: Long): Action[AnyContent] = Action.async {
    eventService.getTasksForEventId(eventId).map {response =>
      ApiResponse.successResult(200, Json.toJson(response))
    }
  }
}
