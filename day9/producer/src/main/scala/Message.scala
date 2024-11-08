


case class Message(messageType: String, message: String, messageKey: String)
case class ProcessMessage(message: String, messageKey: String)

object JsonFormats {
  implicit val messageFormat: RootJsonFormat[Message] = jsonFormat3(Message)
  implicit val processMessageFormat: RootJsonFormat[ProcessMessage] = jsonFormat2(ProcessMessage)

}
