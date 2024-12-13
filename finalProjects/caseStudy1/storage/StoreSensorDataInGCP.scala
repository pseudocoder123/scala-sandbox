package caseStudy1.storage

import caseStudy1.config.Config.DataPaths
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, from_unixtime, struct}
import org.apache.spark.sql.protobuf.functions.to_protobuf
import caseStudy1.config.Config.ProtobufMessageType
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object StoreSensorDataInGCP {
  def storeSensorData(sensorBatchData: DataFrame, currentProcessingTime: LocalDateTime): Unit = {
    /**
     * Store the raw data by serialising it to protobuf
     * The raw data is serialised with respect to the eventTime
     */
    storeRawDataByEventTime(sensorBatchData)

    /**
     * Store the raw data by serialising it to protobuf
     * The raw data is serialised with respect to the processingTime
     */
    storeRawDataByProcessingTime(sensorBatchData, currentProcessingTime)

  }

  def storeRawDataByEventTime(sensorBatchData: DataFrame): Unit = {
    val partitionedDF = sensorBatchData
      .withColumn("value", to_protobuf(struct(sensorBatchData.columns.map(col): _*), ProtobufMessageType.sensorDataMessageType, DataPaths.rawDataProtoDescriptorPath))
      .withColumn("year", from_unixtime(col("timestamp") / 1000, "yyyy")) // Convert milliseconds to seconds
      .withColumn("month", from_unixtime(col("timestamp") / 1000, "MM"))
      .withColumn("day", from_unixtime(col("timestamp") / 1000, "dd"))
      .withColumn("hour", from_unixtime(col("timestamp") / 1000, "HH"))

    // partitionedDF.show(8)
    // partitionedDF.select(col("day")).distinct().show()
    partitionedDF
      .select(col("value"), col("year"), col("month"), col("day"), col("hour"))
      .write
      .mode(SaveMode.Append)
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .save(DataPaths.gsEventTimeBasedRawDataPath)
  }

  def storeRawDataByProcessingTime(sensorBatchData: DataFrame, currentProcessingTime: LocalDateTime): Unit = {
    val formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH")
    val fp = s"${DataPaths.gsProcessingTimeBasedRawDataParentPath}/${currentProcessingTime.format(formatter)}"

    sensorBatchData
      .withColumn("value", to_protobuf(struct(sensorBatchData.columns.map(col): _*), ProtobufMessageType.sensorDataMessageType, DataPaths.rawDataProtoDescriptorPath))
      .select(col("value"))
      .write
      .mode(SaveMode.Append)
      .format("parquet")
      .save(fp)

  }

  def storeAggregatedMetrics(aggregatedMetricsDF: DataFrame, currentProcessingTime: LocalDateTime): Unit = {
    val formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH")

    val aggregatedProtoFP = s"${DataPaths.gsAggregationDataParentPath}/${currentProcessingTime.format(formatter)}"
    val aggregatedJsonFP = s"${DataPaths.gsAggregationJsonDataParentPath}/${currentProcessingTime.format(formatter)}"

    // Store the JSON format
    aggregatedMetricsDF.write.mode(SaveMode.Overwrite).json(aggregatedJsonFP)

    // Store the Proto binary format
    aggregatedMetricsDF
      .withColumn("value", to_protobuf(struct(aggregatedMetricsDF.columns.map(col): _*), ProtobufMessageType.aggregatedMessageType, DataPaths.aggregatedDataProtoDescriptorPath))
      .select(col("value"))
      .write
      .mode(SaveMode.Overwrite)
      .parquet(aggregatedProtoFP)
  }
}
