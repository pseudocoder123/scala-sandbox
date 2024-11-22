package controllers

import models.entity.Issue
import models.response.ApiResponse
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

  /**
   * Creates a new issue in the system.
   * Validates and processes the issue information provided in the JSON payload.
   *
   * @return An Action wrapper containing the HTTP response:
   *         - 201 (Created) with success message and created issue ID
   */
  def create(): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[Issue] match {
      case JsSuccess(issue, _) =>
        issueService.create(issue).map(id =>
        ApiResponse.successResult(201, Json.obj("message"->"Issue created", "id"-> id)))
      case JsError(errors) =>
        Future.successful(ApiResponse.errorResult(
          "Invalid issue request data",
          400
        ))
    }
  }
}
