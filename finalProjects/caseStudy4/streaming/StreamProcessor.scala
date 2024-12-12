package caseStudy4.streaming

import caseStudy4.config.Config.{DataPaths, SparkConfig}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import caseStudy4.config.Config.KafkaConfig
import org.apache.spark.sql.protobuf.functions.from_protobuf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import caseStudy4.metrics.MetricsProcessor.computeHolidaySalesComparison
import org.apache.spark.sql.functions.col

object StreamProcessor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName(SparkConfig.salesStreamAppName)
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", DataPaths.gsServiceAccJsonPath)
        .config("spark.hadoop.fs.gs.auth.service.account.debug", "false")
        .master("local[*]")
        .getOrCreate()

    import spark.implicits._

    // Fully qualified protobuf type
    val messageType = "caseStudy4.protobuf.WeeklySalesData"

    // Read messages from kafka
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KafkaConfig.bootstrapServers)
      .option("subscribe", KafkaConfig.topic)
      .option("startingOffsets", "earliest")
      .load()

    // Extract protobuf binary data from kafka and deserialize it to the df
    val salesDF = kafkaDF
      .selectExpr("CAST(value AS BINARY) as value")
      .select(from_protobuf($"value", messageType, DataPaths.protoDescriptorPath).alias("salesRecord"))
      .select("salesRecord.store", "salesRecord.department", "salesRecord.date", "salesRecord.weekly_sales", "salesRecord.is_holiday")
      .withColumn("date", to_date(col("date").substr(0, 10), "yyyy-MM-dd"))
      .withColumn("weekly_sales", coalesce($"weekly_sales".cast("double"), lit(0.0)))
      .withColumn("is_holiday", coalesce($"is_holiday", lit("FALSE")))

    // salesDF.printSchema()

    // Process this dataframe
    val query = salesDF.writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .foreachBatch { (batchDF: Dataset[Row], _: Long) => processBatch(spark, batchDF) }
      .start()

    query.awaitTermination()
    spark.stop()
  }

  def processBatch(spark: SparkSession, batchDF: DataFrame): Unit = {
    // Append new records to existing weeklySalesData.csv in GCS
    batchDF.write.mode(SaveMode.Append).format("csv").save(DataPaths.gsWeeklySalesDataPath)

    // Update the aggregated metrics to reflect the new records
    updateAggregatedMetrics(spark, batchDF)
  }

  def updateAggregatedMetrics(spark: SparkSession, batchDF: DataFrame): Unit = {
    /**
     * Update Store-Level Aggregate Metrics
     */
    updateStoreLevelMetrics(spark, batchDF)

    /**
     * Update Department-Level Aggregate Metrics
     */
    updateDepartmentLevelMetrics(spark, batchDF)

    /**
     * Update holiday vs non-holiday metrics
     */
    updateHolidaySalesComparisonMetrics(spark, batchDF)

  }

  def updateStoreLevelMetrics(spark: SparkSession, batchDF: DataFrame): Unit = {
    val storeLevelMetricsDF = spark.read.json(DataPaths.gsAggregatedStoreMetrics)
    val batchStoreLevelMetricsDF = batchDF.groupBy("store").agg(
      sum("weekly_sales").alias("total_weekly_sales"),
      round(avg("weekly_sales"), 2).alias("average_weekly_sales"),
      count(lit(1)).alias("no_of_records")
    )

    val updatedStoreLevelMetricsDF = storeLevelMetricsDF.join(batchStoreLevelMetricsDF, Seq("store"), "fullOuter")
      .select(
        storeLevelMetricsDF("store"),
        (coalesce(storeLevelMetricsDF("total_weekly_sales"), lit(0.0)) + coalesce(batchStoreLevelMetricsDF("total_weekly_sales"), lit(0.0))).alias("total_weekly_sales"),
        ((coalesce(storeLevelMetricsDF("total_weekly_sales"), lit(0.0)) + coalesce(batchStoreLevelMetricsDF("total_weekly_sales"), lit(0.0)))/
          (coalesce(storeLevelMetricsDF("no_of_records"), lit(0)) + coalesce(batchStoreLevelMetricsDF("no_of_records"), lit(0)))).alias("average_weekly_sales"),
        (coalesce(storeLevelMetricsDF("no_of_records"), lit(0)) + coalesce(batchStoreLevelMetricsDF("no_of_records"), lit(0))).alias("no_of_records")
      )
      .select(
      col("store"),
      col("total_weekly_sales"),
      round(col("average_weekly_sales"), 2).alias("average_weekly_sales"),
      col("no_of_records")
    )
    // storeLevelMetricsDF.show()
    // updatedStoreLevelMetricsDF.show()

    updatedStoreLevelMetricsDF.write.mode(SaveMode.Overwrite).format("json").save(DataPaths.gsAggregatedStoreMetrics)
  }

  def updateDepartmentLevelMetrics(spark: SparkSession, batchDF: DataFrame): Unit = {
    val departmentLevelMetricsDF = spark.read.json(DataPaths.gsAggregatedDepartmentMetrics)
    val batchDepartmentLevelMetricsDF = batchDF.groupBy("store", "department").agg(sum("weekly_sales").alias("total_weekly_sales"))

    val updatedDepartmentLevelMetricsDF = departmentLevelMetricsDF.join(batchDepartmentLevelMetricsDF, Seq("store", "department"), "fullOuter")
      .select(
        departmentLevelMetricsDF("store"),
        departmentLevelMetricsDF("department"),
        (coalesce(departmentLevelMetricsDF("total_weekly_sales"), lit(0.0)) + coalesce(batchDepartmentLevelMetricsDF("total_weekly_sales"), lit(0.0))).alias("total_weekly_sales")
      )
      .select(
        col("store"),
        col("department"),
        col("total_weekly_sales")
      )
    // departmentLevelMetricsDF.show()
    // updatedDepartmentLevelMetricsDF.show()

    updatedDepartmentLevelMetricsDF.write.mode(SaveMode.Overwrite).format("json").save(DataPaths.gsAggregatedDepartmentMetrics)
  }

  def updateHolidaySalesComparisonMetrics(spark: SparkSession, batchDF: DataFrame): Unit = {
    val holidayComparisonMetricsDF = spark.read.json(DataPaths.gsAggregatedHolidayComparisonSalesMetrics)
    val batchHolidayComparisonMetricsDF = computeHolidaySalesComparison(batchDF)

    val updatedHolidayComparisonMetricsDF = holidayComparisonMetricsDF.join(batchHolidayComparisonMetricsDF, Seq("store", "department"), "fullOuter")
      .select(
        holidayComparisonMetricsDF("store"),
        holidayComparisonMetricsDF("department"),
        (coalesce(holidayComparisonMetricsDF("holiday_sales"), lit(0.0)) + coalesce(batchHolidayComparisonMetricsDF("holiday_sales"), lit(0.0))).alias("holiday_sales"),
        (coalesce(holidayComparisonMetricsDF("non_holiday_sales"), lit(0.0)) + coalesce(batchHolidayComparisonMetricsDF("non_holiday_sales"), lit(0.0))).alias("non_holiday_sales")
      )
      .select(
        col("store"),
        col("department"),
        col("holiday_sales"),
        col("non_holiday_sales")
      )
    // holidayComparisonMetricsDF.show()
    // updatedHolidayComparisonMetricsDF.show()

    updatedHolidayComparisonMetricsDF.write.mode(SaveMode.Overwrite).format("json").save(DataPaths.gsAggregatedHolidayComparisonSalesMetrics)
  }
}
