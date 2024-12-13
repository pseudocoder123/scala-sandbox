package caseStudy1.processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import caseStudy1.config.Config.{DataPaths, ProtobufMessageType}
import caseStudy1.storage.StoreSensorDataInGCP.storeAggregatedMetrics
import org.apache.spark.sql.protobuf.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object ProcessIncrementalAggregation {
  // Incremental Aggregation is performed with respect to the processingTime
  def processBatchDataForIncrementalAggregation(spark: SparkSession, batchDF: DataFrame, currentProcessingTime: LocalDateTime): Unit = {
    import spark.implicits._

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)

    // Check if there is already existing aggregated data in the currentProcessingHour folder in GCP
    // If yes, get this data and overwrite the updated aggregated data, to currentProcessingHour folder
    // Else, get the aggregated data from previousProcessingHour folder, and add the updated aggregated data to currentProcessingHour folder

    def readExistingAggregatedData(fp: String): DataFrame = {
      spark.read.parquet(fp)
        .select(from_protobuf($"value", ProtobufMessageType.aggregatedMessageType, DataPaths.aggregatedDataProtoDescriptorPath).alias("aggregatedData"))
        .select(
          "aggregatedData.sensorId",
          "aggregatedData.averageTemperature",
          "aggregatedData.averageHumidity",
          "aggregatedData.minimumTemperature",
          "aggregatedData.maximumTemperature",
          "aggregatedData.minimumHumidity",
          "aggregatedData.maximumHumidity",
          "aggregatedData.noOfRecords"
        )
    }

    try{
      // Load existing aggregated data
      val previousProcessingTime = currentProcessingTime.minusHours(1)
      val formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH")
      val currentHourAggregatedFolder = s"${DataPaths.gsAggregationDataParentPath}/${currentProcessingTime.format(formatter)}"
      val previousHourAggregatedFolder = s"${DataPaths.gsAggregationDataParentPath}/${previousProcessingTime.format(formatter)}"

      val existingAggregatedData: DataFrame = {
        if (fs.exists(new Path(currentHourAggregatedFolder))) readExistingAggregatedData(currentHourAggregatedFolder)
        else if (fs.exists(new Path(previousHourAggregatedFolder))) readExistingAggregatedData(previousHourAggregatedFolder)
        else null // for the first time aggregation
      }

      if(existingAggregatedData != null) existingAggregatedData.cache()  // repeated usage

      // Perform Incremental Aggregation
      val updatedAggregatedDF = computeAggregatedData(existingAggregatedData, batchDF).cache()  // repeated usage
      // updatedAggregatedDF.show()

      // Store these metrics inside the GCP
      storeAggregatedMetrics(updatedAggregatedDF, currentProcessingTime)
    }
    finally {
      fs.close()
    }
  }

  def computeAggregatedData(existingAggregatedData: DataFrame, batchDF: DataFrame): DataFrame = {
    val batchDataAggregatedDF = getBatchAggregatedDF(batchDF)

    val computedAggData = existingAggregatedData match {
      case null => batchDataAggregatedDF
      case _ =>
        existingAggregatedData.join(batchDataAggregatedDF, Seq("sensorId"), "fullOuterJoin")
        .select(
          coalesce(batchDataAggregatedDF("sensorId"), existingAggregatedData("storeId")).alias("storeId"),
          (
            (
              (coalesce(batchDataAggregatedDF("averageTemperature"), lit(0.0f)) * coalesce(batchDataAggregatedDF("noOfRecords"), lit(0)))
              +(coalesce(existingAggregatedData("averageTemperature"), lit(0.0f)) * coalesce(existingAggregatedData("noOfRecords"), lit(0)))
            )/(
              coalesce(batchDataAggregatedDF("noOfRecords"), lit(0)) + coalesce(existingAggregatedData("noOfRecords"), lit(0))
            )
          ).cast("float")
            .alias("averageTemperature"),
          (
            (
              (coalesce(batchDataAggregatedDF("averageHumidity"), lit(0.0f)) * coalesce(batchDataAggregatedDF("noOfRecords"), lit(0)))
                +(coalesce(existingAggregatedData("averageHumidity"), lit(0.0f)) * coalesce(existingAggregatedData("noOfRecords"), lit(0)))
              )/(
              coalesce(batchDataAggregatedDF("noOfRecords"), lit(0)) + coalesce(existingAggregatedData("noOfRecords"), lit(0))
              )
            ).cast("float")
            .alias("averageHumidity"),
          least(batchDataAggregatedDF("minimumTemperature"), existingAggregatedData("minimumTemperature")).alias("minimumTemperature"),
          greatest(batchDataAggregatedDF("maximumTemperature"), existingAggregatedData("maximumTemperature")).alias("maximumTemperature"),
          least(batchDataAggregatedDF("minimumHumidity"), existingAggregatedData("minimumHumidity")).alias("minimumHumidity"),
          greatest(batchDataAggregatedDF("maximumHumidity"), existingAggregatedData("maximumHumidity")).alias("maximumHumidity"),
          (coalesce(batchDataAggregatedDF("noOfRecords"), lit(0))
            + coalesce(existingAggregatedData("noOfRecords"), lit(0))).alias("noOfRecords")
        )
    }

    computedAggData
  }

  def getBatchAggregatedDF(batchDF: DataFrame): DataFrame = {
    batchDF
      .groupBy("sensorId")
      .agg(
        avg(col("temperature")).cast("float").alias("averageTemperature"),
        avg(col("humidity")).cast("float").alias("averageHumidity"),
        min(col("temperature")).alias("minimumTemperature"),
        max(col("temperature")).alias("maximumTemperature"),
        min(col("humidity")).alias("minimumHumidity"),
        max(col("humidity")).alias("maximumHumidity"),
        count(lit(1)).alias("noOfRecords")
      )
  }
}
