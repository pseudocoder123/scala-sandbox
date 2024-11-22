package models.response

import play.api.libs.json._
import play.api.mvc.Result
import play.api.mvc.Results._

object ApiResponse {
  def success(code: Int, data: JsValue): JsValue = Json.obj(
    "status" -> "success",
    "code" -> code,
    "data" -> data
  )

  def error(message: String, errorCode: Int): JsValue = Json.obj(
    "status" -> "error",
    "error_code" -> errorCode,
    "message" -> message
  )

  def successResult(code: Int, data: JsValue): Result = {
    Ok(success(code, data))
  }

  def errorResult(message: String, statusCode: Int): Result = {
    Status(statusCode)(error(message, statusCode))
  }
}