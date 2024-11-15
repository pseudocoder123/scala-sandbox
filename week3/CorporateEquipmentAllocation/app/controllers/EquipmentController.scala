package controllers

import models.AllocationRequest
import models.entity.Equipment
import play.api.libs.json.{JsError, JsSuccess, JsValue, Json}
import play.api.mvc.{AbstractController, Action, ControllerComponents}
import services.EquipmentService

import javax.inject.Inject
import scala.concurrent.{ExecutionContext, Future}
import play.api.mvc._

class EquipmentController @Inject()(
   val cc: ControllerComponents,
   equipmentService: EquipmentService
)(implicit ec: ExecutionContext) extends AbstractController(cc) {

  // create an equipment
  def createEquipment() : Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[Equipment] match {
      case JsSuccess(equipment, _) =>
        equipmentService.create(equipment).map(created =>
          Created(Json.toJson(created)))
      case JsError(errors) =>
        Future.successful(BadRequest(Json.obj(
          "message" -> "Invalid equipment data",
          "errors" -> JsError.toJson(errors))))
    }
  }

  // update an equipment
  def updateEquipment(id: Long): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[Equipment] match {
      case JsSuccess(equipment, _) =>
        equipmentService.update(id, equipment).map {updated =>
          Ok(Json.toJson(updated))
        }
      case JsError(errors) =>
        Future.successful(BadRequest(Json.obj(
          "message" -> "Invalid equipment data",
          "errors" -> JsError.toJson(errors))))
    }
  }

  // get details of an equipment by Id
  def getEquipmentDetailsById(id: Long): Action[AnyContent] = Action.async {
    equipmentService.getEquipmentById(id).map {equipment =>
      Ok(Json.toJson(equipment))
    }
  }
}
