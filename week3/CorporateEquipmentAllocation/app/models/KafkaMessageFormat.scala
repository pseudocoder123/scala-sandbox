package models

import play.api.libs.functional.syntax._
import play.api.libs.json._

case class KafkaMessageFormat(
                              receiver: String,
                              messageType: String,
                              message: String
                            )

object KafkaMessageFormat {
  private val receiverReads: Reads[String] = (JsPath \ "project").read[String]
  private val messageTypeReads: Reads[String] = (JsPath \ "messageType").read[String]
  private val messageReads: Reads[String] = (JsPath \ "message").read[String]

  // Combine all the reads
  implicit val kafkaMessageFormatReads: Reads[KafkaMessageFormat] = (
    receiverReads and
      messageTypeReads and
      messageReads
    )(KafkaMessageFormat.apply _)

  // Use Json.writes to generate Writes automatically
  implicit val kafkaMessageFormatWrites: Writes[KafkaMessageFormat] = Json.writes[KafkaMessageFormat]

  // Combine Reads and Writes into Format
  implicit val kafkaMessageFormatFormat: Format[KafkaMessageFormat] = Format(kafkaMessageFormatReads,
    kafkaMessageFormatWrites)
}
