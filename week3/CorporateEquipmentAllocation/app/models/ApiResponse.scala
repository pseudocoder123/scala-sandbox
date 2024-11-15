//package models
//
//import play.api.libs.json._
//import play.api.mvc.Result
//import play.api.mvc.Results._
//
//case class ApiResponse[T](status: Int, message: String, data: Option[T])
//
//object ApiResponse {
//  // Generic writes for any type T that has an implicit Writes instance
//  implicit def apiResponseWrites[T](implicit writes: Writes[T]): Writes[ApiResponse[T]] = {
//    new Writes[ApiResponse[T]] {
//      def writes(response: ApiResponse[T]): JsObject = Json.obj(
//        "status" -> response.status,
//        "message" -> response.message,
//        "data" -> response.data
//      )
//    }
//  }
//
//  // Utility methods to create standardized responses
//  def success[T](status: Int, message: String, data: T)(implicit writes: Writes[T]): Result = {
//    Ok(Json.toJson(ApiResponse(status, message, Some(data))))
//  }
//
//  def error(statusCode: Int, message: String, status: Status = BadRequest): Result = {
//    status(Json.toJson(ApiResponse[JsValue](statusCode, message, None)))
//  }
//}
