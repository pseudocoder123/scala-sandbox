package Day18.Exercise3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp, from_json, sum, window}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}
import org.apache.spark.sql.streaming.Trigger

object SparkConsumer {
  def main(args: Array[String]): Unit = {
    // Create a Spark session for structured streaming
    val spark = SparkSession.builder()
      .appName("Exercise3 - KafkaTransactionProcessor")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val topic = "transactions" // kafka-topic

    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    val schema = StructType(Seq(
      StructField("transactionId", IntegerType, nullable = false),
      StructField("userId", IntegerType, nullable = false),
      StructField("amount", IntegerType, nullable = false),
      StructField("timestamp", TimestampType, nullable = false)
    ))

    // Parse the kafka messages
    val parsedStreamDF = kafkaStreamDF
      .selectExpr("CAST(value AS STRING) as jsonString")
      .select(from_json(col("jsonString"), schema).as("data"))
      .select("data.transactionId", "data.amount")
      .withColumn("timestamp", current_timestamp())

    // 10-second window
    val windowAggrs = parsedStreamDF
      .groupBy(window(col("timestamp"), "10 seconds"))
      .agg(sum("amount").alias("total_amount"))

    // Write data to console
    val query = windowAggrs.writeStream
      .outputMode("update") // Only output updated results
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }
}
