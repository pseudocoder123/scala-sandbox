import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Sink
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import java.io.{BufferedWriter, FileWriter}
import spray.json._
import JsonFormats._

object EventManagementMsgReceivers {
  val CATERING = "CATERING"
  val ENTERTAINMENT = "ENTERTAINMENT"
  val DECORATIONS = "DECORATIONS"
  val LOGISTICS = "LOGISTICS"
  val MANAGER = "MANAGER"
}

object CorporateEquipmentAllocationMsgReceivers {
  val MANAGER = "MANAGER"
  val INVENTORY = "INVENTORY"
  val MAINTENANCE = "MAINTENANCE"
  val EMPLOYEE = "EMPLOYEE"
}

object MessageTopics {
  val EVENT_MANAGEMENT_TOPIC = "event-management-topic"
  val CORPORATE_EQUIPMENT_ALLOCATION_TOPIC = "corporate-equipment-allocation-topic"
}

class EventManagementFileWriterActor() extends Actor {
  def receive: Receive = {
    case (fileName: String, messageType: String, message: String) =>
      val bw = new BufferedWriter(new FileWriter(fileName, true))
      bw.write(s"$messageType :: $message")
      bw.newLine()
      bw.close()
  }
}

class CorporateEqpAllocationFileWriterActor() extends Actor {
  def receive: Receive = {
    case (fileName: String, messageType: String, message: String) =>
      val bw = new BufferedWriter(new FileWriter(fileName, true))
      bw.write(s"$messageType :: $message")
      bw.newLine()
      bw.close()
  }
}

class CateringMessageListener(fileWriterActor: ActorRef) extends Actor {
  override def receive: Receive = {
    case msg: KafkaMessageFormat =>
      println("Catering Message Listener consumes the message")
      fileWriterActor ! ("src/main/scala/messages/eventManagement/catering.txt", msg.messageType, msg.message)
  }
}

class EntertainmentMessageListener(fileWriterActor: ActorRef) extends Actor {
  override def receive: Receive = {
    case msg: KafkaMessageFormat =>
      println("Entertainment Message Listener consumes the message")
      fileWriterActor ! ("src/main/scala/messages/eventManagement/entertainment.txt", msg.messageType, msg.message)
  }
}

class DecorationMessageListener(fileWriterActor: ActorRef) extends Actor {
  override def receive: Receive = {
    case msg: KafkaMessageFormat =>
      println("Decoration Message Listener consumes the message")
      fileWriterActor ! ("src/main/scala/messages/eventManagement/decoration.txt", msg.messageType, msg.message)
  }
}

class LogisticsMessageListener(fileWriterActor: ActorRef) extends Actor {
  override def receive: Receive = {
    case msg: KafkaMessageFormat =>
      println("Logistics Message Listener consumes the message")
      fileWriterActor ! ("src/main/scala/messages/eventManagement/logistics.txt", msg.messageType, msg.message)
  }
}

class ManagerMessageListener(fileWriterActor: ActorRef) extends Actor {
  override def receive: Receive = {
    case msg: KafkaMessageFormat =>
      println("Manager Message Listener consumes the message")
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


class ManagerApprovalMessageListener(fileWriterActor: ActorRef) extends Actor {
  override def receive: Receive = {
    case msg: KafkaMessageFormat =>
      println("Manager Approval Message Listener consumes the message")
      fileWriterActor ! ("src/main/scala/messages/corporateEquipmentAllocation/managerApproval.txt", msg.messageType, msg.message)
  }
}

class InventoryMessageListener(fileWriterActor: ActorRef) extends Actor {
  override def receive: Receive = {
    case msg: KafkaMessageFormat =>
      println("Inventory Message Listener consumes the message")
      fileWriterActor ! ("src/main/scala/messages/corporateEquipmentAllocation/inventory.txt", msg.messageType, msg.message)
  }
}

class MaintenanceMessageListener(fileWriterActor: ActorRef) extends Actor {
  override def receive: Receive = {
    case msg: KafkaMessageFormat =>
      println("Maintenance Message Listener consumes the message")
      fileWriterActor ! ("src/main/scala/messages/corporateEquipmentAllocation/maintenance.txt", msg.messageType, msg.message)
  }
}

class EmployeeMessageListener(fileWriterActor: ActorRef) extends Actor {
  override def receive: Receive = {
    case msg: KafkaMessageFormat =>
      println("Employee Message Listener consumes the message")
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


object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("MessagingConsumerSystem")

    val emFileWriterActor: ActorRef = system.actorOf(Props[EventManagementFileWriterActor], "EventManagementFileWriterActor")
    val ceaFileWriterActor: ActorRef = system.actorOf(Props[CorporateEqpAllocationFileWriterActor], "CorporateEqpAllocationFileWriterActor")

    // Create the actors for all the event management listeners
    val cateringMessageListener: ActorRef = system.actorOf(Props(new CateringMessageListener(emFileWriterActor)), "CateringMessageListener")
    val entertainmentMessageListener: ActorRef = system.actorOf(Props(new EntertainmentMessageListener(emFileWriterActor)), "EntertainmentMessageListener")
    val decorationsMessageListener: ActorRef = system.actorOf(Props(new DecorationMessageListener(emFileWriterActor)), "DecorationMessageListener")
    val logisticsMessageListener: ActorRef = system.actorOf(Props(new LogisticsMessageListener(emFileWriterActor)), "LogisticsMessageListener")
    val managerMessageListener: ActorRef = system.actorOf(Props(new ManagerMessageListener(emFileWriterActor)), "ManagerMessageListener")

    // Create the actor for project: event-management
    val eventManagementListener: ActorRef = system.actorOf(Props(new EventManagementListener(
      cateringMessageListener, entertainmentMessageListener, decorationsMessageListener, logisticsMessageListener, managerMessageListener
    )), "EventManagementListener")

    // Create the actors for all the corporate equipment allocation listeners
    val managerApprovalMessageListener: ActorRef = system.actorOf(Props(new ManagerApprovalMessageListener(ceaFileWriterActor)), "ManagerApprovalMessageListener")
    val inventoryMessageListener: ActorRef = system.actorOf(Props(new InventoryMessageListener(ceaFileWriterActor)), "InventoryMessageListener")
    val maintenanceMessageListener: ActorRef = system.actorOf(Props(new MaintenanceMessageListener(ceaFileWriterActor)), "MaintenanceMessageListener")
    val employeeMessageListener: ActorRef = system.actorOf(Props(new EmployeeMessageListener(ceaFileWriterActor)), "EmployeeMessageListener")

    // Create the actor for project: corporate-equipment-allocation
    val corporateEquipAllocationListener: ActorRef = system.actorOf(Props(new CorporateEquipAllocation(
      managerApprovalMessageListener, inventoryMessageListener, maintenanceMessageListener, employeeMessageListener)), "CorporateEquipAllocationListener")



    val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers("localhost"+":9092")
      .withGroupId("group1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    // Create and start the consumers (i.e, messageListeners)
    def listeners(topic: String, listener: ActorRef): Unit = {
      Consumer
        .plainSource(consumerSettings, Subscriptions.topics(topic))
        .map{ record => record.value().parseJson.convertTo[KafkaMessageFormat] }
        .runWith(
          Sink.actorRef[KafkaMessageFormat](
            ref = listener,
            onCompleteMessage = "complete",
            onFailureMessage = (throwable: Throwable) => s"Exception encountered"
          )
        )
    }

    // Configure listeners
    listeners(MessageTopics.EVENT_MANAGEMENT_TOPIC, eventManagementListener)
    listeners(MessageTopics.CORPORATE_EQUIPMENT_ALLOCATION_TOPIC, corporateEquipAllocationListener)
  }
}