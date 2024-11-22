package controllers

import models.entity.Equipment
import models.response.ApiResponse
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

  /**
   * Creates a new equipment record in the system.
   * Validates and processes the equipment information provided in the JSON payload.
   *
   * @return An Action wrapper containing the HTTP response:
   *         - 201 (Created) with success message and created equipment ID
   */
  def createEquipment() : Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[Equipment] match {
      case JsSuccess(equipment, _) =>
        equipmentService.create(equipment).map(created =>
          ApiResponse.successResult(201, Json.obj("message"->"Equipment created", "id"-> created))
        ).recover{
          case ex: Exception =>
            ApiResponse.errorResult(s"Error creating the equipment: ${ex.getMessage}", 400)
        }
      case JsError(errors) =>
        Future.successful(ApiResponse.errorResult(
          "Invalid equipment data",
          400
        ))
    }
  }

  /**
   * Updates an existing equipment record identified by its ID.
   */
  def updateEquipment(id: Long): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[Equipment] match {
      case JsSuccess(equipment, _) =>
        equipmentService.update(id, equipment).map {updated =>
          ApiResponse.successResult(200, Json.toJson(updated))
        }
      case JsError(errors) =>
        Future.successful(ApiResponse.errorResult(
          "Invalid equipment data",
          400
        ))
    }
  }

  /**
   * Retrieves detailed information for specific equipment by its ID.
   *
   * @param id The unique identifier of the equipment to retrieve
   * @return An Action wrapper containing the HTTP response:
   *         - 200 (OK) with the equipment details as JSON
   */
  def getEquipmentDetailsById(id: Long): Action[AnyContent] = Action.async {
    equipmentService.getEquipmentById(id).map {equipment =>
      ApiResponse.successResult(200, Json.toJson(equipment))
    }
  }
}
