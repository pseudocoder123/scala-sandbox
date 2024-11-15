package controllers

import models.entity.{Issue, Team}
import play.api.mvc._
import play.api.libs.json._
import services.IssueService
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class IssueController @Inject()(
                                val cc: ControllerComponents,
                                issueService: IssueService
                              )(implicit ec: ExecutionContext) extends AbstractController(cc) {

  // Register a team
  def create(): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[Issue] match {
      case JsSuccess(issue, _) =>
        issueService.create(issue).map(id =>
          Created(Json.obj("id" -> id, "message" -> "CREATED")))
      case JsError(errors) =>
        Future.successful(BadRequest(Json.obj(
          "message" -> "Invalid Issue data",
          "errors" -> JsError.toJson(errors))))
    }
  }
}
