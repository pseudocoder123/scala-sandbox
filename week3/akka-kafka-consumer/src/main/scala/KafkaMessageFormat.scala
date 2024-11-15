import spray.json.DefaultJsonProtocol.{StringJsonFormat, jsonFormat3}
import spray.json.RootJsonFormat

case class KafkaMessageFormat(
                               receiver: String,
                               messageType: String,
                               message: String
                             )

object JsonFormats {
  implicit val kafkaMessageFormat: RootJsonFormat[KafkaMessageFormat] = jsonFormat3(KafkaMessageFormat)

}

