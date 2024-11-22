package utils

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Sink
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import models.KafkaMessageFormat
import spray.json._
import constants.MessageTopics
import actors._
import akka.stream.{ActorMaterializer, Materializer}
import config.EnvConfig
import models.JsonFormats.kafkaMessageFormat

object KafkaConsumer {
  def startConsumers(system: ActorSystem): Unit = {
    implicit val materializer: Materializer = ActorMaterializer()(system)

    val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(EnvConfig.getKafkaBroker)
      .withGroupId("group1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    // Define file writer actors
    val emFileWriterActor: ActorRef = system.actorOf(Props[EventManagementFileWriterActor], "EventManagementFileWriterActor")
    val ceaFileWriterActor: ActorRef = system.actorOf(Props[CorporateEqpAllocationFileWriterActor], "CorporateEqpAllocationFileWriterActor")

    // Configure Event Management Listeners
    val eventManagementListener = system.actorOf(Props(new EventManagementListener(
      system.actorOf(Props(new CateringMessageListener(emFileWriterActor))),
      system.actorOf(Props(new EntertainmentMessageListener(emFileWriterActor))),
      system.actorOf(Props(new DecorationMessageListener(emFileWriterActor))),
      system.actorOf(Props(new LogisticsMessageListener(emFileWriterActor))),
      system.actorOf(Props(new ManagerMessageListener(emFileWriterActor)))
    )))

    // Configure Corporate Equipment Allocation Listeners
    val corporateEquipAllocationListener = system.actorOf(Props(new CorporateEquipAllocation(
      system.actorOf(Props(new ManagerApprovalMessageListener(ceaFileWriterActor))),
      system.actorOf(Props(new InventoryMessageListener(ceaFileWriterActor))),
      system.actorOf(Props(new MaintenanceMessageListener(ceaFileWriterActor))),
      system.actorOf(Props(new EmployeeMessageListener(ceaFileWriterActor)))
    )))

    // Start Kafka listeners
    startListener(consumerSettings, MessageTopics.EVENT_MANAGEMENT_TOPIC, eventManagementListener)
    startListener(consumerSettings, MessageTopics.CORPORATE_EQUIPMENT_ALLOCATION_TOPIC, corporateEquipAllocationListener)
  }

  private def startListener(consumerSettings: ConsumerSettings[String, String], topic: String, listener: ActorRef)
                           (implicit materializer: Materializer): Unit = {
    Consumer
      .plainSource(consumerSettings, Subscriptions.topics(topic))
      .map(record => record.value().parseJson.convertTo[KafkaMessageFormat])
      .runWith(Sink.actorRef[KafkaMessageFormat](listener, onCompleteMessage = "complete", onFailureMessage = (throwable: Throwable) => s"Exception encountered"))
  }
}
