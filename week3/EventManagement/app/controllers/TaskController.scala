package controllers

import models.entity.Task
import models.enums.TaskStatus
import models.request.AssignTasksRequest
import models.response.ApiResponse
import play.api.mvc._
import play.api.libs.json._
import services.TaskService
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class TaskController @Inject()(
     val cc: ControllerComponents,
     taskService: TaskService
)(implicit ec: ExecutionContext) extends AbstractController(cc) {

  /**
   * Creates a new task in the system.
   * Validates and processes the task information provided in the JSON payload.
   *
   * @return An Action wrapper containing the HTTP response:
   *         - 201 (Created) with success message and created task ID
   */
  def createTask(): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[Task] match {
      case JsSuccess(task, _) =>
        taskService.create(task).map(created =>
          ApiResponse.successResult(201, Json.obj("message"->"Task created", "id"-> created)))
      case JsError(errors) =>
        Future.successful(ApiResponse.errorResult(
          "Invalid task request data",
          400
        ))
    }
  }

  /**
   * Retrieves detailed information for a specific task by its ID.
   *
   * @param taskId The unique identifier of the task to retrieve
   * @return An Action wrapper containing the HTTP response:
   *         - 200 (OK) with the task details as JSON
   */
  def getTaskById(taskId: Long): Action[AnyContent] = Action.async {
    taskService.getTaskById(taskId).map(created =>
      ApiResponse.successResult(200, Json.toJson(created)))
  }

  /**
   * Updates the status of an existing task.
   * Converts the status string to TaskStatus enum before processing.
   *
   * @param taskId The unique identifier of the task to update
   * @param status The new status to be applied (must match TaskStatus enum values)
   * @return An Action wrapper containing the HTTP response:
   *         - 200 (OK) with the updated task details
   *
   * Note: Status must be a valid TaskStatus enum value
   */
  def updateTaskStatus(taskId: Long, status: String): Action[AnyContent] = Action.async {
    val taskStatus = TaskStatus.withNameOption(Some(status))
    taskService.updateStatus(taskId, taskStatus.get).map(updated =>
      ApiResponse.successResult(200, Json.toJson(updated)))
  }

  /**
   * Assigns tasks to teams for specified eventId.
   * Processes bulk task assignments in a single request.
   *
   * @return An Action wrapper containing the HTTP response:
   *         - 200 (OK) with the assignment results
   */
  def assignTasks(): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[AssignTasksRequest] match {
      case JsSuccess(req, _) =>
        taskService.assignTasks(req).map(created =>
          ApiResponse.successResult(200, Json.toJson(created))
        )
      case JsError(errors) =>
        Future.successful(ApiResponse.errorResult(
          "Invalid task request data",
          400
        ))
    }
  }

}
