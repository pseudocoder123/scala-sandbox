package services

import models.KafkaMessageFormat
import models.entity.{Allocation, Employee}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import play.api.libs.json._

import java.time.LocalDate
import javax.inject._
import java.util.Properties

object MessageTeam {
  val MANAGER = "MANAGER"
  val INVENTORY = "INVENTORY"
  val MAINTENANCE = "MAINTENANCE"
  val EMPLOYEE = "EMPLOYEE"
}

@Singleton
class KafkaProducerFactory @Inject()() {
  private val props = new Properties()

  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  private val producer = new KafkaProducer[String, String](props)

  def sendAllocationApprovalRequest(request: Allocation): Unit = {
    val record: ProducerRecord[String, String] = {
      var message = s"${request.employeeId} raised allocationRequest(${request.id.get}) for ${request.equipmentType} on ${request.requestDate}"
      if(request.purpose.isDefined) message += s" mentioned purpose: ${request.purpose.get}"

      val kafkaMessageFormat = KafkaMessageFormat(
        receiver=MessageTeam.MANAGER,
        messageType="ALLOCATION_REQUEST",
        message= message
      )

      val jsonMessage: String = Json.stringify(Json.toJson(kafkaMessageFormat))
      new ProducerRecord[String, String]("corporate-equipment-allocation-topic", jsonMessage)
    }

    producer.send(record)
  }

  def sendInventoryUpdateMessage(request: Allocation, action: String): Unit = {
    val record: ProducerRecord[String, String] = {
      var message = if(action.equals("ALLOCATED")) {
        s"Equipment(${request.equipmentId.get}) is allocated to allocation(${request.id.get}) " +
          s"for employee(${request.employeeId}) expected to return on: ${request.expectedReturnDate.get}"
      } else {
        s"Equipment(${request.equipmentId.get}) is returned for allocation(${request.id.get}) on ${request.returnDate.get}"
      }

      val messageType = if (action.equals("ALLOCATED")) "EQUIPMENT_ALLOCATED" else "EQUIPMENT_RETURNED"

      val kafkaMessageFormat = KafkaMessageFormat(
        receiver=MessageTeam.INVENTORY,
        messageType=messageType,
        message= message
      )

      val jsonMessage: String = Json.stringify(Json.toJson(kafkaMessageFormat))
      new ProducerRecord[String, String]("corporate-equipment-allocation-topic", jsonMessage)
    }

    producer.send(record)
  }

  def sendMaintenanceNotification(allocation: Allocation, reportedDate: LocalDate): Unit = {
    val record: ProducerRecord[String, String] = {
      var message = s"Maintenance required for equipmentId: ${allocation.equipmentId.get} as part of allocationId: ${allocation.id.get}," +
        s" reportedOn: ${reportedDate}"

      val kafkaMessageFormat = KafkaMessageFormat(
        receiver=MessageTeam.MAINTENANCE,
        messageType="MAINTENANCE_REQUEST",
        message= message
      )

      val jsonMessage: String = Json.stringify(Json.toJson(kafkaMessageFormat))
      new ProducerRecord[String, String]("corporate-equipment-allocation-topic", jsonMessage)
    }

    producer.send(record)
  }

  def sendMessageOverdueNotification(allocation: Allocation, employee: Employee): Unit = {
    val record: ProducerRecord[String, String] = {
      var message = s"Overdue for equipment(${allocation.equipmentId.get}), expected ReturnDate(${allocation.expectedReturnDate.get}) for employeeId(${allocation.employeeId})"

      val kafkaMessageFormat = KafkaMessageFormat(
        receiver = MessageTeam.EMPLOYEE,
        messageType = "OVERDUE_NOTIFICATION",
        message = message
      )

      val jsonMessage: String = Json.stringify(Json.toJson(kafkaMessageFormat))
      new ProducerRecord[String, String]("corporate-equipment-allocation-topic", jsonMessage)
    }

    producer.send(record)
  }

}