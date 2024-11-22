import akka.actor.ActorSystem
import utils.KafkaConsumer

object MainApp extends App {
  implicit val system = ActorSystem("MessagingConsumerSystem")
  KafkaConsumer.startConsumers(system)
}
