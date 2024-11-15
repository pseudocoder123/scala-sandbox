package controllers

import models.{AllocationRequest, MaintenanceUpdateRequest}
import models.entity.Equipment
import play.api.libs.json.{JsError, JsSuccess, JsValue, Json}
import play.api.mvc.{AbstractController, Action, ControllerComponents}
import services.{EquipmentService, MaintenanceService}

import javax.inject.Inject
import scala.concurrent.{ExecutionContext, Future}
import play.api.mvc._

class MaintenanceController @Inject()(
                                     val cc: ControllerComponents,
                                     maintenanceService: MaintenanceService
                                   )(implicit ec: ExecutionContext) extends AbstractController(cc) {

  // update an equipment
  def updateMaintenanceStatusAction(): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[MaintenanceUpdateRequest] match {
      case JsSuccess(req, _) =>
        maintenanceService.updateMaintenanceStatus(req).map(created =>
          Created(Json.toJson(created)))
      case JsError(errors) =>
        Future.successful(BadRequest(Json.obj(
          "message" -> "Invalid person data",
          "errors" -> JsError.toJson(errors))))
    }
  }
}
