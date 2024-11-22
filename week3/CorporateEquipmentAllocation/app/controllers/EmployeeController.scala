package controllers

import models.entity.Employee
import models.request.AllocationApprovalRequest
import models.response.ApiResponse
import play.api.mvc._
import play.api.libs.json._
import services.EmployeeService
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class EmployeeController @Inject()(
    val cc: ControllerComponents,
    employeeService: EmployeeService
)(implicit ec: ExecutionContext) extends AbstractController(cc) {

  /**
   * Creates a new employee record in the system.
   * Validates and processes the employee information provided in the JSON payload.
   *
   * @return An Action wrapper containing the HTTP response:
   *         - 201 (Created) with the created employee ID and success message
   */
  def createEmployee(): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[Employee] match {
      case JsSuccess(employee, _) =>
        employeeService.create(employee).map { created =>
          ApiResponse.successResult(201, Json.obj("message"->"Employee created", "id"-> created))
        }.recover {
          case ex: Exception =>
            ApiResponse.errorResult(s"Error creating employee: ${ex.getMessage}", 500)
            InternalServerError(Json.obj("message" -> "Error creating employee"))
        }
      case JsError(errors) =>
        Future.successful(
          ApiResponse.errorResult(
            "Invalid employee data",
            400
          )
        )
    }
  }

  /**
   * Retrieves all allocation requests for a specific employee.
   * Lists both pending and processed allocation requests associated with the employee.
   *
   * @param employeeId The unique identifier of the employee
   * @return An Action wrapper containing the HTTP response:
   *         - 200 (OK) with JSON array of allocation requests
   */
  def getAllocationRequests(employeeId: Long): Action[AnyContent] = Action.async {
    employeeService.getAllocationsByEmpId(employeeId).map(data =>
      ApiResponse.successResult(200, Json.toJson(data)))
  }

  /**
   * Retrieves all equipment approval requests raised by employees under a specific manager
   * Returns allocation requests from employees reporting to the specified manager.
   *
   * @param employeeId The unique identifier of the manager
   * @return An Action wrapper containing the HTTP response:
   *         - 200 (OK) with JSON array of pending approval requests
   */
  def getApprovalRequests(employeeId: Long): Action[AnyContent] = Action.async {
    employeeService.getApprovalRequests(employeeId).map(data =>
      ApiResponse.successResult(200, Json.toJson(data)))
  }

  /**
   * Processes approval or rejection of an allocation request by a manager.
   * Updates the allocation request status based on the manager's decision.
   */
  def allocationRequestApproval(): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[AllocationApprovalRequest] match {
      case JsSuccess(request, _) =>
        employeeService.updateAllocationApproval(request).map(data => {
          val message = if(request.isApproved) "Allocation Request is Approved" else "Allocation Request is Rejected"
          ApiResponse.successResult(201,
            Json.obj(
              "message"-> s"Updated: $message"
            ))}
        ).recover {
          case ex: Exception =>
            ApiResponse.errorResult(s"Error approving the allocationRequest: ${ex.getMessage}", 500)
        }
      case JsError(errors) =>
        Future.successful(
          ApiResponse.errorResult(
            "Invalid AllocationApprovalRequest data",
            400
          )
        )
    }
  }
}
