package actors

import akka.actor.{Actor, ActorRef}
import config.EnvConfig
import models.KafkaMessageFormat
import org.slf4j.LoggerFactory
import constants.CorporateEquipmentAllocationMsgReceivers
import utils.EmailUtils

class CorporateEqpAllocationFileWriterActor() extends Actor {
  def receive: Receive = {
    case (fileName: String, messageType: String, message: String) =>
      EmailUtils.sendEmail(EnvConfig.getEmailRecipient, messageType, message)
  }
}


class ManagerApprovalMessageListener(fileWriterActor: ActorRef) extends Actor {
  private val logger = LoggerFactory.getLogger("ManagerApprovalMessageLogger")

  override def receive: Receive = {
    case msg: KafkaMessageFormat =>
      println("Manager Approval Message Listener consumes the message")
      logger.info(s"${msg.messageType} :: ${msg.message}")
      fileWriterActor ! ("src/main/scala/messages/corporateEquipmentAllocation/managerApproval.txt", msg.messageType, msg.message)
  }
}

class InventoryMessageListener(fileWriterActor: ActorRef) extends Actor {
  private val logger = LoggerFactory.getLogger("InventoryMessageLogger")

  override def receive: Receive = {
    case msg: KafkaMessageFormat =>
      println("Inventory Message Listener consumes the message")
      logger.info(s"${msg.messageType} :: ${msg.message}")
      fileWriterActor ! ("src/main/scala/messages/corporateEquipmentAllocation/inventory.txt", msg.messageType, msg.message)
  }
}

class MaintenanceMessageListener(fileWriterActor: ActorRef) extends Actor {
  private val logger = LoggerFactory.getLogger("MaintenanceMessageLogger")

  override def receive: Receive = {
    case msg: KafkaMessageFormat =>
      println("Maintenance Message Listener consumes the message")
      logger.info(s"${msg.messageType} :: ${msg.message}")
      fileWriterActor ! ("src/main/scala/messages/corporateEquipmentAllocation/maintenance.txt", msg.messageType, msg.message)
  }
}

class EmployeeMessageListener(fileWriterActor: ActorRef) extends Actor {
  private val logger = LoggerFactory.getLogger("EmployeeMessageLogger")

  override def receive: Receive = {
    case msg: KafkaMessageFormat =>
      println("Employee Message Listener consumes the message")
      logger.info(s"${msg.messageType} :: ${msg.message}")
      fileWriterActor ! ("src/main/scala/messages/corporateEquipmentAllocation/employee.txt", msg.messageType, msg.message)
  }
}

class CorporateEquipAllocation(managerApprovalMessageListener: ActorRef,
                               inventoryMessageListener: ActorRef,
                               maintenanceMessageListener: ActorRef,
                               employeeMessageListener: ActorRef
                              )extends Actor {
  override def receive: Receive = {
    case msg: KafkaMessageFormat => msg.receiver match {
      case CorporateEquipmentAllocationMsgReceivers.MANAGER =>
        managerApprovalMessageListener ! msg
      case CorporateEquipmentAllocationMsgReceivers.INVENTORY =>
        inventoryMessageListener ! msg
      case CorporateEquipmentAllocationMsgReceivers.MAINTENANCE =>
        maintenanceMessageListener ! msg
      case CorporateEquipmentAllocationMsgReceivers.EMPLOYEE =>
        employeeMessageListener ! msg
    }
  }

}
