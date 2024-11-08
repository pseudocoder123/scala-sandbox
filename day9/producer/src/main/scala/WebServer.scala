

// Message Types
object MessageTypes {
  val NETWORK_MESSAGE = "NETWORK"
  val APP_MESSAGE = "APP"
  val CLOUD_MESSAGE = "CLOUD"
  val NETWORK_MESSAGE_TOPIC = "network-message"
  val APP_MESSAGE_TOPIC = "app-message"
  val CLOUD_MESSAGE_TOPIC = "cloud-message"
}

/**
 * Msg -> MessageHandler -> Processors -> Producer
 */

// Message Handler
class MessageHandler(networkMessageActor: ActorRef, appMessageActor: ActorRef, cloudMessageActor: ActorRef) extends Actor {
  def receive: Receive = {
    // Receive the message and based on message type call the corresponding actor
    case Message(messageType, message, messageKey) =>
      messageType match {
        case MessageTypes.NETWORK_MESSAGE =>
          println(s"MessageHandler: Routing message to Network Message Handler")
          networkMessageActor ! ProcessMessage(message, messageKey)
        case MessageTypes.APP_MESSAGE =>
          println(s"MessageHandler: Routing message to App Message Handler")
          appMessageActor ! ProcessMessage(message, messageKey)
        case MessageTypes.CLOUD_MESSAGE =>
          println(s"MessageHandler: Routing message to Cloud Message Handler")
          cloudMessageActor ! ProcessMessage(message, messageKey)
      }
  }
}

// Network Message Processor
class NetworkMessageProcessor(producer: KafkaProducer[String, String]) extends Actor {
  def receive: Receive = {
    case pm: ProcessMessage =>
      println(s"NetworkMessageProcessor: Received the message from the MessageHandler")

      val processMessageJsonString: String = pm.toJson.toString() // convert to JSON string
      val record = new ProducerRecord[String, String](MessageTypes.NETWORK_MESSAGE_TOPIC, processMessageJsonString)
      producer.send(record)
      println(s"NetworkMessageProcessor produced message: $processMessageJsonString")
  }
}

// App Message Processor
class AppMessageProcessor(producer: KafkaProducer[String, String]) extends Actor {
  def receive: Receive = {
    case pm: ProcessMessage =>
      println(s"AppMessageProcessor: Received the message from the MessageHandler")

      val processMessageJsonString: String = pm.toJson.toString() // convert to JSON string
      val record = new ProducerRecord[String, String](MessageTypes.APP_MESSAGE_TOPIC, processMessageJsonString)
      producer.send(record)
      println(s"AppMessageProcessor produced message: $processMessageJsonString")
  }
}

// Cloud Message Processor
class CloudMessageProcessor(producer: KafkaProducer[String, String]) extends Actor {
  def receive: Receive = {
    case pm: ProcessMessage =>
      println(s"CloudMessageProcessor: Received the message from the MessageHandler")

      val processMessageJsonString: String = pm.toJson.toString() // convert to JSON string
      val record = new ProducerRecord[String, String](MessageTypes.CLOUD_MESSAGE_TOPIC, processMessageJsonString)
      producer.send(record)
      println(s"CloudMessageProcessor produced message: $processMessageJsonString")
  }
}



object WebServer {
  implicit val system = ActorSystem("MessagingSystem")  // ActorSystem
  val producer: KafkaProducer[String, String] = KafkaProducerFactory.createProducer()

  // Create Processor Actors
  val networkMessageProcessor: ActorRef = system.actorOf(Props(new NetworkMessageProcessor(producer)), "NetworkMessageProcessor")
  val appMessageProcessor: ActorRef = system.actorOf(Props(new AppMessageProcessor(producer)), "AppMessageProcessor")
  val cloudMessageProcessor: ActorRef = system.actorOf(Props(new CloudMessageProcessor(producer)), "CloudMessageProcessor")

  // Create MessageHandler Actor
  val messageHandler: ActorRef = system.actorOf(Props(new MessageHandler(networkMessageProcessor, appMessageProcessor, cloudMessageProcessor)))

  def sendKafkaMessage(message: Message) = {
    // Initiate the message processing
    messageHandler ! message
  }

  def sendMultipleMessages(messages: List[Message]) = {
    messages.foreach(msg =>
      messageHandler ! msg
    )
  }

  def main(args: Array[String]): Unit = {
    val route = concat(
      post {
        path("process-message") {
          entity(as[Message]) { message =>
            sendKafkaMessage(message)
            complete(StatusCodes.OK, s"Message sent to Kafka: $message")
          }
        }
      },
      post {
        path("process-multiple-messages") {
          entity(as[List[Message]]) { messages =>
            sendMultipleMessages(messages)
            complete(StatusCodes.OK, s"Message sent to Kafka: $messages")
          }
        }
      }
    )

    Http().newServerAt("0.0.0.0", 8080).bind(route)
    println("Server online at http://0.0.0.0:8080/")
  }
}

