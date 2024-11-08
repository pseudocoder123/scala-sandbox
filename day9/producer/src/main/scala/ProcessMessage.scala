
case class ProcessMessage(message: String, messageKey: String)

object JsonFormats {
  implicit val processMessageFormat: RootJsonFormat[ProcessMessage] = jsonFormat2(ProcessMessage)

}