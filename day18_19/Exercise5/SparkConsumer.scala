package Day18.Exercise5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

object SparkConsumer {
  def main(args: Array[String]): Unit = {
    val gsServiceAccJsonPath = "<PATH>/spark-gcs-key.json"

    val gsUserCsvPath = "gs://spark-tasks-bucket/day_18_19/exercise5/user_details"
    val gsEnrichedDataOutputPath = "gs://spark-tasks-bucket/day_18_19/exercise5/enriched_data"
    val gsEnrichedDataCheckpointPath = "gs://spark-tasks-bucket/day_18_19/exercise5/enriched_data_checkpoint"

    val topic = "orders"

    val spark = SparkSession.builder()
      .appName("User Details CSV To GCS")
      .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
      .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
      .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gsServiceAccJsonPath)
      .config("spark.hadoop.fs.gs.auth.service.account.debug", "true")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") // set log level to warn

    // userDetails dataframe
    val userDetailsDF = spark.read.parquet(gsUserCsvPath)
    userDetailsDF.show(10)

    // kafkaStream dataframe
    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    val ordersSchema = StructType(Seq(
      StructField("order_id", IntegerType),
      StructField("user_id", IntegerType),
      StructField("amount", DoubleType)
    ))

    // Parse the kafka dataframe
    val parsedDF = kafkaStreamDF
      .selectExpr("CAST(value AS STRING) as json_string")
      .select(from_json(col("json_string"), ordersSchema).as("data"))
      .select("data.order_id", "data.user_id", "data.amount")

    // Enrich the data
    val enrichedDF = parsedDF
      .join(broadcast(userDetailsDF), Seq("user_id"), "left_outer") // some orders may not contain user_id details
      .select(
        col("order_id"),
        col("user_id"),
        col("amount"),
        coalesce(col("name"), lit("UNKNOWN")).alias("name"), // Default name
        coalesce(col("age"), lit(0)).alias("age"), // Default age
        coalesce(col("email"), lit("NOT_AVAILABLE")).alias("email") // Default email
      )

    // Write the stream to GCS
    val query = enrichedDF.writeStream
      .outputMode("append") // Append mode for continuous data
      .format("json") // Write as JSON
      .option("path", gsEnrichedDataOutputPath)
      .option("checkpointLocation", gsEnrichedDataCheckpointPath)
      .start()

    query.awaitTermination()
  }
}
