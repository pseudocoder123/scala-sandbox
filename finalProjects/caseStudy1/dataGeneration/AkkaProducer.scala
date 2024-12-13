package caseStudy1.dataGeneration

import akka.actor.ActorSystem
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import caseStudy1.config.Config.KafkaConfig
import akka.kafka.ProducerSettings
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import scala.util.Random
import scalapb.GeneratedMessage
import akka.stream.scaladsl.Source
import caseStudy1.protobuf.SensorReading.SensorReading
import akka.kafka.scaladsl.Producer
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global

object AkkaProducer {
  def main(args: Array[String]): Unit = {
    val random = new Random()
    var messagesCount = 0

    // Initialise ActorSystem
    implicit val actorSystem: ActorSystem = ActorSystem("IoTSensorProducer")

    val producerSettings: ProducerSettings[String, Array[Byte]] = {
      ProducerSettings(actorSystem, new StringSerializer, new ByteArraySerializer)
        .withBootstrapServers(KafkaConfig.bootstrapServers)
    }

    def serializeProtobuf[T <: GeneratedMessage](message: T): Array[Byte] = message.toByteArray

    def generateTimeStamp(): Long = {
      val daysToSubtract = random.nextInt(2) + 1
      val millisInDay = 24 * 60 * 60 * 1000L

      val currentMillis = System.currentTimeMillis()

      currentMillis - (daysToSubtract * millisInDay)
    }

    val createAndSendRecord: () => ProducerRecord[String, Array[Byte]] = () => {
      val sensorReading: SensorReading = {
        val sensorId = random.nextInt(10) + 1 // sensorId: [0, 10]
        val timestamp = generateTimeStamp()
        val temperature = -50 + random.nextFloat() * 200 // temperature: [-50, 150]
        val humidity = random.nextFloat() * 100 // humidity: [0, 100]

        SensorReading(sensorId=sensorId, timestamp=timestamp, temperature=temperature, humidity=humidity)
      }

      val serializedSensorReading = serializeProtobuf(sensorReading)
      val record = new ProducerRecord[String, Array[Byte]](KafkaConfig.topic, serializedSensorReading)
      messagesCount += 1
      println(s"Message $messagesCount: $sensorReading sent at ${LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))}")

      record
    }

    val records = Source.tick(FiniteDuration(0, TimeUnit.MILLISECONDS), FiniteDuration(100, TimeUnit.MILLISECONDS), ()).map {_ =>
      createAndSendRecord()
    }

    records.runWith(Producer.plainSink(producerSettings))
      .onComplete {result =>
        println(s"Kafka Producer completed with result: $result")
        actorSystem.terminate()
      }

  }
}
