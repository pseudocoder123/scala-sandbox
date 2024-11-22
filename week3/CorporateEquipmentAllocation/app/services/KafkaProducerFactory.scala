package services

import models.entity.{Allocation, Employee, Equipment, Maintenance}
import models.enums.EquipmentCondition.EquipmentCondition
import models.request.KafkaMessageFormat
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import play.api.libs.json._
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import javax.inject._
import java.util.Properties
import config.EnvConfig

object MessageTeam {
  val MANAGER = "MANAGER"
  val INVENTORY = "INVENTORY"
  val MAINTENANCE = "MAINTENANCE"
  val EMPLOYEE = "EMPLOYEE"
}

@Singleton
class KafkaProducerFactory @Inject()() {
  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val TOPIC = "corporate-equipment-allocation-topic"

  private val producer: KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", EnvConfig.getKafkaBroker)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](props)
  }

  private def createRecord(messageFormat: KafkaMessageFormat): ProducerRecord[String, String] = {
    val jsonMessage = Json.stringify(Json.toJson(messageFormat))
    new ProducerRecord[String, String](TOPIC, jsonMessage)
  }

  def sendAllocationApprovalRequest(request: Allocation): Unit = {
    val message = {
      val baseMessage =
        s"""Employee ${request.employeeId} has requested allocation #${request.id.get}
           |for ${request.equipmentType} on ${request.requestDate.format(dateFormatter)}""".stripMargin

      request.purpose.map(purpose => s"$baseMessage with purpose: $purpose")
        .getOrElse(baseMessage)
    }

    val kafkaMessage = KafkaMessageFormat(
      receiver = MessageTeam.MANAGER,
      messageType = "ALLOCATION_REQUEST",
      message = message
    )

    producer.send(createRecord(kafkaMessage))
  }

  def sendInventoryUpdateMessage(allocation: Allocation, action: String): Unit = {
    val (messageType, message) = action match {
      case "ALLOCATED" => (
        "EQUIPMENT_ALLOCATED",
        s"""Equipment #${allocation.equipmentId.get} has been allocated to request #${allocation.id.get}.
           |Employee ${allocation.employeeId} is expected to return it by ${allocation.expectedReturnDate.get.format(dateFormatter)}""".stripMargin
      )
      case _ => (
        "EQUIPMENT_RETURNED",
        s"""Equipment #${allocation.equipmentId.get} has been returned for request #${allocation.id.get}
           |on ${allocation.returnDate.get.format(dateFormatter)}""".stripMargin
      )
    }

    val kafkaMessage = KafkaMessageFormat(
      receiver = MessageTeam.INVENTORY,
      messageType = messageType,
      message = message
    )

    producer.send(createRecord(kafkaMessage))
  }

  def sendMaintenanceNotification(allocation: Allocation, reportedDate: LocalDate): Unit = {
    val message =
      s"""Maintenance request for Equipment #${allocation.equipmentId.get}
         |Allocation ID: ${allocation.id.get}
         |Reported Date: ${reportedDate.format(dateFormatter)}""".stripMargin

    val kafkaMessage = KafkaMessageFormat(
      receiver = MessageTeam.MAINTENANCE,
      messageType = "MAINTENANCE_REQUEST",
      message = message
    )

    producer.send(createRecord(kafkaMessage))
  }

  def sendMessageOverdueNotification(allocation: Allocation, employee: Employee): Unit = {
    val message =
      s"""OVERDUE NOTICE
         |Equipment: #${allocation.equipmentId.get}
         |Employee ID: ${allocation.employeeId}
         |Expected Return Date: ${allocation.expectedReturnDate.get.format(dateFormatter)}
         |Please return the equipment immediately.""".stripMargin

    val kafkaMessage = KafkaMessageFormat(
      receiver = MessageTeam.EMPLOYEE,
      messageType = "OVERDUE_NOTIFICATION",
      message = message
    )

    producer.send(createRecord(kafkaMessage))
  }

  def sendInventoryUpdateAfterMaintenance(eq: Equipment, eqStatus: EquipmentCondition, maintenance: Maintenance): Unit = {
    val message =
      s"""Equipment #${eq.id.get} under maintenance #${maintenance.id.get} is ${eqStatus.toString}. The maintenance request
         |was raised on ${maintenance.reportedDate}""".stripMargin

    val kafkaMessage = KafkaMessageFormat(
      receiver = MessageTeam.INVENTORY,
      messageType = "MAINTENANCE_SERVICE_UPDATE",
      message = message
    )

    producer.send(createRecord(kafkaMessage))
  }

  def sendEmployeeAllocationApprovalStatus(employeeId: Long, allocationId: Long, isApproved: Boolean): Unit = {
    val status = if(isApproved) "APPROVED" else "REJECTED"
    val message =
      s"""Hello, #${employeeId}. Your allocation request status: #${allocationId} is ${status}""".stripMargin

    val kafkaMessage = KafkaMessageFormat(
      receiver = MessageTeam.EMPLOYEE,
      messageType = "ALLOCATION_STATUS_UPDATE",
      message = message
    )

    producer.send(createRecord(kafkaMessage))
  }

}