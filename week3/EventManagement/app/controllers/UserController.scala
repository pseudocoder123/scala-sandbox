package controllers

import models.entity.User
import models.response.ApiResponse
import play.api.libs.json._
import play.api.mvc._
import services.UserService
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class UserController @Inject()(
                                    val cc: ControllerComponents,
                                    userService: UserService
                                  )(implicit ec: ExecutionContext) extends AbstractController(cc) {

  /**
   * Creates a new user record in the system.to manage authorization
   * Validates and processes the employee information provided in the JSON payload.
   *
   * @return An Action wrapper containing the HTTP response:
   *         - 201 (Created) with the created user ID and success message
   */
  def createUser(): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[User] match {
      case JsSuccess(user, _) =>
        userService.create(user).map { created =>
          ApiResponse.successResult(201, Json.obj("message"->"User created", "id"-> created))
        }.recover {
          case ex: Exception =>
            ApiResponse.errorResult(s"Error creating user: ${ex.getMessage}", 500)
            InternalServerError(Json.obj("message" -> "Error creating user"))
        }
      case JsError(errors) =>
        Future.successful(
          ApiResponse.errorResult(
            "Invalid user data",
            400
          )
        )
    }
  }
}
