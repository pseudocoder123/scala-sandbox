import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Sink
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import spray.json._
import JsonFormats._

import java.util.Properties

/**
 * Msg -> Consumers -> MessageGatherer -> Producer
 */

object Constants {
  val NETWORK_MESSAGE_TOPIC = "network-message"
  val APP_MESSAGE_TOPIC = "app-message"
  val CLOUD_MESSAGE_TOPIC = "cloud-message"
  val CONSOLIDATED_MESSAGES_TOPIC = "consolidated-messages"
}


class NetworkListener(messageGatherer: ActorRef) extends Actor {
  override def receive: Receive = {
    case pm: ProcessMessage =>
      println("Network Listener consumes the message")
      messageGatherer ! pm
  }
}

class AppListener(messageGatherer: ActorRef) extends Actor {
  override def receive: Receive = {
    case pm: ProcessMessage =>
      println("App Listener consumes the message")
      messageGatherer ! pm
  }
}

class CloudListener(messageGatherer: ActorRef) extends Actor {
  override def receive: Receive = {
    case pm: ProcessMessage =>
      println("Cloud Listener consumes the message")
      messageGatherer ! pm
  }
}


class MessageGatherer(producer: KafkaProducer[String, String]) extends Actor {
  override def receive: Receive = {
    case pm: ProcessMessage =>
      println("MessageGatherer received the processMessage")
      val processMessageJsonString: String = pm.toJson.toString() // convert to JSON string
      val record = new ProducerRecord[String, String](Constants.CONSOLIDATED_MESSAGES_TOPIC, processMessageJsonString)
      producer.send(record)
      println(s"MessageGatherer produced message: $processMessageJsonString to topic: consolidated-messages")
  }
}

object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("MessagingConsumerSystem")

    // Create the producer
    val producer: KafkaProducer[String, String] = {
      val props = new Properties()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost"+":9092")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      new KafkaProducer[String, String](props)
    }

    // Create the MessageGatherer Actor
    val messageGatherer: ActorRef = system.actorOf(Props(new MessageGatherer(producer)), "MessageGatherer")

    // Create the actors for listeners
    val networkListener: ActorRef = system.actorOf(Props(new NetworkListener(messageGatherer)), "NetworkMessageListener")
    val appListener: ActorRef = system.actorOf(Props(new AppListener(messageGatherer)), "AppMessageListener")
    val cloudListener: ActorRef = system.actorOf(Props(new CloudListener(messageGatherer)), "CloudMessageListener")

    val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers("localhost"+":9092")
      .withGroupId("group1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    // Create and start the consumers (i.e, messageListeners)
    def listeners(topic: String, listener: ActorRef): Unit = {
      Consumer
        .plainSource(consumerSettings, Subscriptions.topics(topic))
        .map{ record => record.value().parseJson.convertTo[ProcessMessage] }
        .runWith(
          Sink.actorRef[ProcessMessage](
            ref = listener,
            onCompleteMessage = "complete",
            onFailureMessage = (throwable: Throwable) => s"Exception encountered"
          )
        )
    }

    // Configure listeners
    listeners(Constants.NETWORK_MESSAGE_TOPIC, networkListener)
    listeners(Constants.APP_MESSAGE_TOPIC, appListener)
    listeners(Constants.CLOUD_MESSAGE_TOPIC, cloudListener)
  }
}
