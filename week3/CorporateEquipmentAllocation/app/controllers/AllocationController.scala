package controllers

import models.{AllocationRequest, ReturnEquipment}
import models.entity.Equipment
import play.api.libs.json.{JsError, JsSuccess, JsValue, Json}
import play.api.mvc.{AbstractController, Action, ControllerComponents}
import services.{AllocationService, EquipmentService}

import javax.inject.Inject
import scala.concurrent.{ExecutionContext, Future}
import play.api.mvc._

class AllocationController @Inject()(
     val cc: ControllerComponents,
     allocationService: AllocationService
)(implicit ec: ExecutionContext) extends AbstractController(cc) {

  // request equipment allocation
  def requestAllocation(): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[AllocationRequest] match {
      case JsSuccess(allocation, _) =>
        allocationService.requestEquipment(allocation).map(created =>
          Created(Json.toJson(created)))
      case JsError(errors) =>
        Future.successful(BadRequest(Json.obj(
          "message" -> "Invalid Allocation Request data",
          "errors" -> JsError.toJson(errors))))
    }
  }

  // get allocation details by allocation id
  def getAllocationDetails(allocationId: Long): Action[AnyContent] = Action.async {
    allocationService.getAllocationDetails(allocationId).map(created =>
      Ok(Json.toJson(created))
    )
  }

  // process the allocation request
  def processAllocation(allocationId: Long): Action[AnyContent] = Action.async {
    allocationService.processAllocation(allocationId).map(updated =>
      Ok(Json.toJson(updated))
    )
  }

  // process the equipment returned
  def processReturnEquipment(): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[ReturnEquipment] match {
      case JsSuccess(req, _) =>
        allocationService.returnEquipment(req).map(returned =>
          Ok(Json.toJson(returned)))
      case JsError(errors) =>
        Future.successful(BadRequest(Json.obj(
          "message" -> "Invalid Return Equipment Request data",
          "errors" -> JsError.toJson(errors))))
    }
  }
}
