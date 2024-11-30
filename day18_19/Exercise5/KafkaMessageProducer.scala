package Day18.Exercise5

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.util.Random
import java.util.Properties
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object KafkaMessageProducer {
  def main(args: Array[String]): Unit = {
    val topic = "orders"
    val random = new Random()

    val producer: KafkaProducer[String, String] = {
      val props = new Properties()
      props.put("bootstrap.servers", "localhost:9092")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      new KafkaProducer[String, String](props)
    }

    val sendRecord: () => Unit = () => {
      val (orderId, userId, amount) = (
        random.nextInt(1000) + 1,
        100 + random.nextInt(100) + 1,
        BigDecimal(random.nextDouble() * 500).setScale(2, BigDecimal.RoundingMode.HALF_UP)
      )

      val msg = s"""{"order_id": $orderId, "user_id": $userId, "amount": $amount}"""
      val record = new ProducerRecord[String, String](topic, msg)
      producer.send(record)
      println(s"Message: $msg sent at ${LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))}")
    }

    val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
    scheduler.scheduleAtFixedRate(() => sendRecord(), 0, 1, TimeUnit.SECONDS)

    // Add shutdown hook for graceful termination
    sys.addShutdownHook {
      println("Shutting down Kafka producer and scheduler...")
      scheduler.shutdown() // Stop the scheduler
      try {
        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
          scheduler.shutdownNow() // Force shutdown if tasks don’t terminate in time
        }
      } catch {
        case _: InterruptedException =>
          scheduler.shutdownNow() // Force shutdown on interrupt
      }
      producer.close() // Close Kafka producer
      println("Kafka producer stopped.")
    }
  }
}
