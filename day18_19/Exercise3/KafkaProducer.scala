package Day18.Exercise3

import com.google.gson.{Gson, JsonParser}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.collection.convert.ImplicitConversions.`iterator asScala`
import scala.io.Source

object KafkaProducer {
  def main(args: Array[String]): Unit = {
    val topic = "transactions"
    val fp = "src/main/scala/Day18/transactions.json"

    val producer: KafkaProducer[String, String] = {
      val props = new Properties()
      props.put("bootstrap.servers", "localhost:9092")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      new KafkaProducer[String, String](props)
    }

    try {
      val messages = readJsonFile(fp)
      messages.foreach{msg =>
        producer.send(new ProducerRecord[String, String](topic, msg))
        Thread.sleep(1000)
      }
    }
    finally{
      producer.close()
    }

    def readJsonFile(filePath: String): List[String] = {
      var source: Source = null
      try {
        source = Source.fromFile(filePath)
        val content = source.mkString

        val gson = new Gson()
        val jsonElement = JsonParser.parseString(content)

        if (jsonElement.isJsonArray) {
          jsonElement.getAsJsonArray.iterator()
            .map(elem => gson.toJson(elem))
            .toList
        } else {
          List(gson.toJson(jsonElement))
        }
      }
      catch {
        case e: Exception =>
          println(s"Error reading file: ${e.getMessage}")
          List.empty
      } finally {
        if (source != null) source.close()
      }
    }
  }
}
