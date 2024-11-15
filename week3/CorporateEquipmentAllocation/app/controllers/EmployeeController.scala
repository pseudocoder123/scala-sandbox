package controllers

import models.entity.{Allocation, Employee}
import models.{AllocationApprovalRequest, AllocationRequest}
import play.api.mvc._
import play.api.libs.json._
import services.{EmployeeService, EquipmentService}

import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class EmployeeController @Inject()(
    val cc: ControllerComponents,
    employeeService: EmployeeService
)(implicit ec: ExecutionContext) extends AbstractController(cc) {

  // create an employee (testing purpose)
  def createEmployee(): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[Employee] match {
      case JsSuccess(employee, _) =>
        employeeService.create(employee).map { created =>
          Created(Json.toJson(created))
        }.recover {
          case ex: Exception =>
            InternalServerError(Json.obj("message" -> "Error creating employee"))
        }
      case JsError(errors) =>
        Future.successful(BadRequest(Json.obj(
          "message" -> "Invalid employee data",
          "errors" -> JsError.toJson(errors))))
    }
  }

  // list employee allocation requests
  def getAllocationRequests(employeeId: Long): Action[AnyContent] = Action.async {
    employeeService.getAllocationsByEmpId(employeeId).map(created =>
      Ok(Json.toJson(created)))
  }

  // Get all the allocation requests from his employees
  def getApprovalRequests(employeeId: Long): Action[AnyContent] = Action.async {
    employeeService.getApprovalRequests(employeeId).map(created =>
      Ok(Json.toJson(created)))
  }

  // Approve/ Reject the allocation request
  def allocationRequestApproval(): Action[JsValue] = Action.async(parse.json) { request =>
    request.body.validate[AllocationApprovalRequest] match {
      case JsSuccess(request, _) =>
        employeeService.updateAllocationApproval(request).map(created =>
          Created(Json.toJson(created)))
      case JsError(errors) =>
        Future.successful(BadRequest(Json.obj(
          "message" -> "Invalid employee data",
          "errors" -> JsError.toJson(errors))))
    }
  }
}
