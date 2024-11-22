package controllers

import models.request.{AllocationRequest, ReturnEquipment}
import models.response.ApiResponse
import play.api.libs.json.{JsError, JsSuccess, JsValue, Json}
import play.api.mvc.{AbstractController, Action, ControllerComponents}
import services.AllocationService
import javax.inject.Inject
import scala.concurrent.{ExecutionContext, Future}
import play.api.mvc._

class AllocationController @Inject()(
     val cc: ControllerComponents,
     allocationService: AllocationService
)(implicit ec: ExecutionContext) extends AbstractController(cc) {

  /**
   * Create equipment allocation requests record by validating and processing JSON payload
   * @return An Action wrapper containing the HTTP response:
   *         - 201 with created `AllocationResponse` containing allocationId
   */
  def requestAllocation(): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[AllocationRequest] match {
      case JsSuccess(allocation, _) =>
        allocationService.requestEquipment(allocation).map{created =>
          ApiResponse.successResult(201, Json.toJson(created))
        }.recover {
          case ex: Exception =>
            ApiResponse.errorResult(s"Error creating allocationRequest: ${ex.getMessage}", 500)
        }
      case JsError(errors) =>
        Future.successful(
          ApiResponse.errorResult(
            "Invalid Allocation Request data",
            400
          ))
    }
  }

  /**
   * Retrieves detailed information for a specific allocation by its ID.
   *
   * @param allocationId The unique identifier of the allocation to retrieve
   * @return An Action wrapper containing the HTTP response:
   *         - 200 (OK) with the allocation details as JSON
   */
  def getAllocationDetails(allocationId: Long): Action[AnyContent] = Action.async {
    allocationService.getAllocationDetails(allocationId).map(created =>
      ApiResponse.successResult(200, Json.toJson(created))
    )
  }

  /**
   * Processes a allocation request identified by the allocation ID.
   * This endpoint allocates the equipment based on the availability
   *
   * @param allocationId The unique identifier of the allocation to process
   * @return An Action wrapper containing the HTTP response:
   *         - 200 (OK) with the updated `AllocationResponse` details
   */
  def processAllocation(allocationId: Long): Action[AnyContent] = Action.async {
    allocationService.processAllocation(allocationId).map{updated =>
      ApiResponse.successResult(200, Json.toJson(updated))
    }.recover{
      case ex: Exception =>
        ApiResponse.errorResult(s"Error processing allocationRequest: ${ex.getMessage}", 500)
    }
  }

  /**
   * Handles the equipment return process by validating and processing the return request.
   * Expects a JSON payload containing return details including allocationId and maintenanceRequirement boolean.
   *
   * @return An Action wrapper containing the HTTP response:
   *         - 200 (OK) with the processed return details `ReturnEquipmentResponse`
   */
  def processReturnEquipment(): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[ReturnEquipment] match {
      case JsSuccess(req, _) =>
        allocationService.returnEquipment(req).map{returned =>
          ApiResponse.successResult(200, Json.toJson(returned))
        }.recover {
          case ex: Exception =>
            ApiResponse.errorResult(s"Error processing allocationRequest: ${ex.getMessage}", 500)
        }
      case JsError(errors) =>
        Future.successful(BadRequest(Json.obj(
          "message" -> "Invalid Return Equipment Request data",
          "errors" -> JsError.toJson(errors))))
    }
  }
}
