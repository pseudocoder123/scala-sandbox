package caseStudy4.messageProducer

import caseStudy4.protobuf.WeeklySalesData.WeeklySalesData
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.time.LocalDateTime
import scalapb.GeneratedMessage
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.util.Random
import caseStudy4.config.Config.KafkaConfig

object KafkaMessageProducer {
  def main(args: Array[String]): Unit = {
    val random = new Random()

    val producer: KafkaProducer[String, Array[Byte]] = {
      val props = new Properties()
      props.put("bootstrap.servers", KafkaConfig.bootstrapServers)
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
      new KafkaProducer[String, Array[Byte]](props)
    }

    // Serialize Protobuf to byte array
    def serializeProtobuf[T <: GeneratedMessage](message: T): Array[Byte] = message.toByteArray

    val createAndSendRecord: () => Unit = () => {
      val weeklySalesData: WeeklySalesData = {
        val store = random.nextInt(5) + 1
        val department = random.nextInt(10) + 1
        val date = java.time.LocalDateTime.now().toString
        val weeklySales = 1000 + scala.util.Random.nextFloat() * 1000
        val isHoliday = if(scala.util.Random.nextBoolean()) "TRUE" else "FALSE"

        WeeklySalesData(store = store, department = department, date = date, weeklySales = weeklySales, isHoliday = isHoliday)
      }

      val serializedWeeklySalesData = serializeProtobuf(weeklySalesData)
      val record = new ProducerRecord[String, Array[Byte]](KafkaConfig.topic, serializedWeeklySalesData)

      producer.send(record)
      println(s"Message: $weeklySalesData sent at ${LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))}")
    }

    val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
    scheduler.scheduleAtFixedRate(() => createAndSendRecord(), 0, 1, TimeUnit.SECONDS)

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
