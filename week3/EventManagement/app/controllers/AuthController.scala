package controllers

import play.api.libs.json._
import play.api.mvc._
import security.JwtUtil
import services.UserService
import javax.inject.Inject
import scala.concurrent.ExecutionContext

class AuthController @Inject()(cc: ControllerComponents, userService: UserService)
                              (implicit executionContext: ExecutionContext) extends AbstractController(cc) {

  def login: Action[JsValue] = Action.async(parse.json) { request =>
    val username = (request.body \ "username").as[String]
    val password = (request.body \ "password").as[String]

    userService.verifyUser(username, password).map {
      case true =>
        val token = JwtUtil.generateToken(username)
        Ok(Json.obj("token" -> token))
      case false =>
        Unauthorized(Json.obj("error" -> "Invalid credentials"))
    }
  }
}