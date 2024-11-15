package controllers

import models.AssignTasksRequest
import models.entity.Task
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

  // Create a task
  def createTask(): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[Task] match {
      case JsSuccess(task, _) =>
        taskService.create(task).map(created =>
          Created(Json.toJson(created)))
      case JsError(errors) =>
        Future.successful(BadRequest(Json.obj(
          "message" -> "Invalid Event data",
          "errors" -> JsError.toJson(errors))))
    }
  }

  // Get task details
  def getTaskById(taskId: Long): Action[AnyContent] = Action.async {
    taskService.getEventById(taskId).map(created =>
      Ok(Json.toJson(created)))
  }

  // Update task details
  def updateTaskStatus(taskId: Long, status: String): Action[AnyContent] = Action.async {
    taskService.updateStatus(taskId, status).map(updated =>
    Ok(Json.toJson(updated)))
  }

  // Assign tasks
  def assignTasks(): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[AssignTasksRequest] match {
      case JsSuccess(req, _) =>
        taskService.assignTasks(req).map(created =>
          Created(Json.toJson(created)))
      case JsError(errors) =>
        Future.successful(BadRequest(Json.obj(
          "message" -> "Invalid Event data",
          "errors" -> JsError.toJson(errors))))
    }
  }

}
