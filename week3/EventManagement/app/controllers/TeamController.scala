package controllers

import models.entity.Team
import models.enums.TeamType
import models.response.ApiResponse
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

  /**
   * Registers a new team in the system.
   * Validates and processes the team information provided in the JSON payload.
   *
   * @return An Action wrapper containing the HTTP response:
   *         - 201 (Created) with success message and created team ID
   */
  def registerTeam(): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[Team] match {
      case JsSuccess(team, _) =>
        teamService.create(team).map(created =>
          ApiResponse.successResult(201, Json.obj("message"->"Team created", "id"-> created)))
      case JsError(errors) =>
        Future.successful(ApiResponse.errorResult(
          "Invalid team request data",
          400
        ))
    }
  }

  /**
   * Retrieves detailed information for a specific team by its ID.
   * Returns comprehensive team details including members and related information.
   *
   * @param teamId The unique identifier of the team to retrieve
   * @return An Action wrapper containing the HTTP response:
   *         - 200 (OK) with the team details as JSON
   */
  def getTeamDetails(teamId: Long): Action[AnyContent] = Action.async {
    teamService.getTeamDetailsById(teamId).map(created =>
      ApiResponse.successResult(200, Json.toJson(created)))
  }

  /**
   * Retrieves a filtered list of teams based on optional team type.
   * Converts the provided team type string to TeamType enum for filtering.
   *
   * @param teamType Optional type of team to filter by (must match TeamType enum values)
   * @return An Action wrapper containing the HTTP response:
   *         - 200 (OK) with JSON array of matching teams
   */
  def listTeams(teamType: Option[String]): Action[AnyContent] = Action.async {
    val teamTypeEnum = TeamType.withNameOption(teamType)
    teamService.listTeams(teamTypeEnum).map(created =>
      ApiResponse.successResult(200, Json.toJson(created)))
  }
}
