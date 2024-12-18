package caseStudy1.streaming

import caseStudy1.config.Config.{DataPaths, KafkaConfig, ProtobufMessageType, SparkConfig}
import caseStudy1.processing.ProcessIncrementalAggregation.processBatchDataForIncrementalAggregation
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.protobuf.functions._
import org.apache.spark.sql.streaming.Trigger
import caseStudy1.storage.StoreSensorDataInGCP.storeSensorData
import org.apache.spark.sql.functions._
import java.time.LocalDateTime

object StreamProcessor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(SparkConfig.sensorDataStreamAppName)
      .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      .config("spark.hadoop.fs.defaultFS", DataPaths.gsDefaultFS)
      .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
      .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
      .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", DataPaths.gsServiceAccJsonPath)
      .config("spark.hadoop.fs.gs.auth.service.account.debug", "false")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Read messages from kafka
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KafkaConfig.bootstrapServers)
      .option("subscribe", KafkaConfig.topic)
      .option("startingOffsets", "earliest")
      .load()

    // Extract the protobuf binary data from kafka and deserialize it to df and validate the df
    val sensorDataValidatedDF = kafkaDF
      .selectExpr("CAST(value AS BINARY) as value")
      .select(from_protobuf($"value", ProtobufMessageType.sensorDataMessageType, DataPaths.rawDataProtoDescriptorPath).alias("sensorDataRecord"))
      .select("sensorDataRecord.sensorId", "sensorDataRecord.timestamp", "sensorDataRecord.temperature", "sensorDataRecord.humidity")
      .filter($"temperature".between(-40, 140) && $"humidity".between(0, 80) && $"sensorId".isNotNull) // Filter for these values

    // Process this dataframe
    val query = sensorDataValidatedDF.writeStream
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .foreachBatch { (batchDF: Dataset[Row], _: Long) => processBatch(spark, batchDF) }
      .start()

    query.awaitTermination()
    spark.stop()
  }

  def processBatch(spark: SparkSession, batchDF: DataFrame): Unit = {
    /**
     * batchDF is small-sized dataframe[only 100 rows], unlike the batchDF in caseStudy4 that can be fully copied to all the executor nodes
     */

//    // Persist the batchDF to avoid multiple computations
//    batchDF.cache()
//    try{
//      // batchDF.show()
//      processSensorStreamData(spark, batchDF)
//    }finally {
//      batchDF.unpersist()
//    }

    val broadcastBatchDF = broadcast(batchDF)
    // broadcastBatchDF.printSchema()
    // broadcastBatchDF.show()

    val currentProcessingTime = LocalDateTime.now()
    storeSensorData(broadcastBatchDF, currentProcessingTime)
    processBatchDataForIncrementalAggregation(spark, broadcastBatchDF, currentProcessingTime)
  }
}
