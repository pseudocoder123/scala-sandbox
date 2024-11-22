package controllers

import models.request.MaintenanceUpdateRequest
import models.response.ApiResponse
import play.api.libs.json.{JsError, JsSuccess, JsValue, Json}
import play.api.mvc.{AbstractController, Action, ControllerComponents}
import services.MaintenanceService
import javax.inject.Inject
import scala.concurrent.{ExecutionContext, Future}

class MaintenanceController @Inject()(
                                     val cc: ControllerComponents,
                                     maintenanceService: MaintenanceService
                                   )(implicit ec: ExecutionContext) extends AbstractController(cc) {

  /**
   * Updates the maintenance status of equipment.
   */
  def updateMaintenanceStatusAction(): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[MaintenanceUpdateRequest] match {
      case JsSuccess(req, _) =>
        maintenanceService.updateMaintenanceStatus(req).map(created =>
          ApiResponse.successResult(201, Json.obj("message"->"Maintenance status updated")))
      case JsError(errors) =>
        Future.successful(ApiResponse.errorResult(
          "Invalid maintenance request data",
          400
        ))
    }
  }
}
