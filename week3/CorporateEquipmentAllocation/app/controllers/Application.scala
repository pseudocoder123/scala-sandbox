package controllers

import javax.inject._
import play.api.mvc._

@Singleton
class Application @Inject()(cc: ControllerComponents) extends AbstractController(cc) {

  def preflight(path: String) = Action { request =>
    NoContent.withHeaders(
      "Access-Control-Allow-Origin" -> "*",
      "Access-Control-Allow-Methods" -> "POST, GET, OPTIONS, PUT, DELETE",
      "Access-Control-Allow-Headers" -> "Accept, Content-Type, Origin, X-Auth-Token"
    )
  }
}