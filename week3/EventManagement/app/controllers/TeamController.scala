package controllers

import models.entity.Team
import play.api.mvc._
import play.api.libs.json._
import services.TeamService

import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class TeamController @Inject()(
                                    val cc: ControllerComponents,
                                    teamService: TeamService
                                  )(implicit ec: ExecutionContext) extends AbstractController(cc) {

  // Register a team
  def registerTeam(): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[Team] match {
      case JsSuccess(team, _) =>
        teamService.create(team).map(created =>
          Created(Json.toJson(created)))
      case JsError(errors) =>
        Future.successful(BadRequest(Json.obj(
          "message" -> "Invalid Team data",
          "errors" -> JsError.toJson(errors))))
    }
  }

  // Get Team Details
  def getTeamDetails(teamId: Long): Action[AnyContent] = Action.async {
    teamService.getTeamDetailsById(teamId).map(created =>
      Ok(Json.toJson(created)))
  }

  // List Teams
  def listTeams(teamType: Option[String]): Action[AnyContent] = Action.async {
    teamService.listTeams(teamType).map(created =>
      Ok(Json.toJson(created)))
  }
}
