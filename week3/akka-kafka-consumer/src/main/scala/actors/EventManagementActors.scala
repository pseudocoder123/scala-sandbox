package actors

import akka.actor.{Actor, ActorRef}
import config.EnvConfig
import constants.EventManagementMsgReceivers
import models.KafkaMessageFormat
import org.slf4j.LoggerFactory
import utils.EmailUtils

class EventManagementFileWriterActor() extends Actor {
  def receive: Receive = {
    case (fileName: String, messageType: String, message: String) =>
      EmailUtils.sendEmail(EnvConfig.getEmailRecipient, messageType, message)
  }
}

class CateringMessageListener(fileWriterActor: ActorRef) extends Actor {
  private val logger = LoggerFactory.getLogger("CateringMessageLogger")

  override def receive: Receive = {
    case msg: KafkaMessageFormat =>
      println("Catering Message Listener consumes the message")
      logger.info(s"${msg.messageType} :: ${msg.message}")
      fileWriterActor ! ("src/main/scala/messages/eventManagement/catering.txt", msg.messageType, msg.message)
  }
}

class EntertainmentMessageListener(fileWriterActor: ActorRef) extends Actor {
  private val logger = LoggerFactory.getLogger("EntertainmentMessageLogger")

  override def receive: Receive = {
    case msg: KafkaMessageFormat =>
      println("Entertainment Message Listener consumes the message")
      logger.info(s"${msg.messageType} :: ${msg.message}")
      fileWriterActor ! ("src/main/scala/messages/eventManagement/entertainment.txt", msg.messageType, msg.message)
  }
}

class DecorationMessageListener(fileWriterActor: ActorRef) extends Actor {
  private val logger = LoggerFactory.getLogger("DecorationMessageLogger")

  override def receive: Receive = {
    case msg: KafkaMessageFormat =>
      println("Decoration Message Listener consumes the message")
      logger.info(s"${msg.messageType} :: ${msg.message}")
      fileWriterActor ! ("src/main/scala/messages/eventManagement/decoration.txt", msg.messageType, msg.message)
  }
}

class LogisticsMessageListener(fileWriterActor: ActorRef) extends Actor {
  private val logger = LoggerFactory.getLogger("LogisticsMessageLogger")

  override def receive: Receive = {
    case msg: KafkaMessageFormat =>
      println("Logistics Message Listener consumes the message")
      logger.info(s"${msg.messageType} :: ${msg.message}")
      fileWriterActor ! ("src/main/scala/messages/eventManagement/logistics.txt", msg.messageType, msg.message)
  }
}

class ManagerMessageListener(fileWriterActor: ActorRef) extends Actor {
  private val logger = LoggerFactory.getLogger("ManagerMessageLogger")

  override def receive: Receive = {
    case msg: KafkaMessageFormat =>
      println("Manager Message Listener consumes the message")
      logger.info(s"${msg.messageType} :: ${msg.message}")
      fileWriterActor ! ("src/main/scala/messages/eventManagement/manager.txt", msg.messageType, msg.message)
  }
}

class EventManagementListener(cateringMessageListener: ActorRef,
                              entertainmentMessageListener: ActorRef,
                              decorationMessageListener: ActorRef,
                              logisticsMessageListener: ActorRef,
                              managerMessageListener: ActorRef
                             )extends Actor {
  override def receive: Receive = {
    case msg: KafkaMessageFormat => msg.receiver match {
      case EventManagementMsgReceivers.CATERING =>
        cateringMessageListener ! msg
      case EventManagementMsgReceivers.ENTERTAINMENT =>
        entertainmentMessageListener ! msg
      case EventManagementMsgReceivers.DECORATIONS =>
        decorationMessageListener ! msg
      case EventManagementMsgReceivers.LOGISTICS =>
        logisticsMessageListener ! msg
      case EventManagementMsgReceivers.MANAGER =>
        managerMessageListener ! msg
    }
  }
}
