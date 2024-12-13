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
    // Persist the batchDF to avoid multiple computations
    batchDF.cache()

    try{
      // Append new records to existing weeklySalesData.csv in GCS
      batchDF.write.mode(SaveMode.Append).format("csv").save(DataPaths.gsWeeklySalesDataPath)

      // Update the aggregated metrics to reflect the new records
      updateAggregatedMetrics(spark, batchDF)
    }finally {
      // Unpersist the cached DataFrame
      batchDF.unpersist()
    }
  }

  def updateAggregatedMetrics(spark: SparkSession, batchDF: DataFrame): Unit = {
    val storeAggs = batchDF.groupBy("store").agg(
      sum("weekly_sales").alias("total_weekly_sales"),
      round(avg("weekly_sales"), 2).alias("average_weekly_sales"),
      count(lit(1)).alias("no_of_records")
    )

    val departmentAggs = batchDF.groupBy("store", "department").agg(
      sum("weekly_sales").alias("total_weekly_sales")
    )

    val holidayComparison = computeHolidaySalesComparison(batchDF)

    /**
     * Update Store-Level Aggregate Metrics
     */
    updateStoreLevelMetrics(spark, storeAggs)

    /**
     * Update Department-Level Aggregate Metrics
     */
    updateDepartmentLevelMetrics(spark, departmentAggs)

    /**
     * Update holiday vs non-holiday metrics
     */
    updateHolidaySalesComparisonMetrics(spark, holidayComparison)

  }

  def updateStoreLevelMetrics(spark: SparkSession, batchStoreAggsDF: DataFrame): Unit = {
    val storeLevelMetricsDF = spark.read.json(DataPaths.gsAggregatedStoreMetrics)

    val updatedStoreLevelMetricsDF = storeLevelMetricsDF.join(batchStoreAggsDF, Seq("store"), "fullOuter")
      .select(
        coalesce(storeLevelMetricsDF("store"), batchStoreAggsDF("store")).alias("store"),
        (coalesce(storeLevelMetricsDF("total_weekly_sales"), lit(0.0)) + coalesce(batchStoreAggsDF("total_weekly_sales"), lit(0.0))).alias("total_weekly_sales"),
        ((coalesce(storeLevelMetricsDF("total_weekly_sales"), lit(0.0)) + coalesce(batchStoreAggsDF("total_weekly_sales"), lit(0.0)))/
          (coalesce(storeLevelMetricsDF("no_of_records"), lit(0)) + coalesce(batchStoreAggsDF("no_of_records"), lit(0)))).alias("average_weekly_sales"),
        (coalesce(storeLevelMetricsDF("no_of_records"), lit(0)) + coalesce(batchStoreAggsDF("no_of_records"), lit(0))).alias("no_of_records")
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

  def updateDepartmentLevelMetrics(spark: SparkSession, batchDepartmentAggsDF: DataFrame): Unit = {
    val departmentLevelMetricsDF = spark.read.json(DataPaths.gsAggregatedDepartmentMetrics)

    val updatedDepartmentLevelMetricsDF = departmentLevelMetricsDF.join(batchDepartmentAggsDF, Seq("store", "department"), "fullOuter")
      .select(
        coalesce(departmentLevelMetricsDF("store"), batchDepartmentAggsDF("store")).alias("store"),
        coalesce(departmentLevelMetricsDF("department"), batchDepartmentAggsDF("department")).alias("department"),
        (coalesce(departmentLevelMetricsDF("total_weekly_sales"), lit(0.0)) + coalesce(batchDepartmentAggsDF("total_weekly_sales"), lit(0.0))).alias("total_weekly_sales")
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

  def updateHolidaySalesComparisonMetrics(spark: SparkSession, batchHolidayComparisonDF: DataFrame): Unit = {
    val holidayComparisonMetricsDF = spark.read.json(DataPaths.gsAggregatedHolidayComparisonSalesMetrics)

    val updatedHolidayComparisonMetricsDF = holidayComparisonMetricsDF.join(batchHolidayComparisonDF, Seq("store", "department"), "fullOuter")
      .select(
        coalesce(holidayComparisonMetricsDF("store"), batchHolidayComparisonDF("store")).alias("store"),
        coalesce(holidayComparisonMetricsDF("department"), batchHolidayComparisonDF("department")).alias("department"),
        (coalesce(holidayComparisonMetricsDF("holiday_sales"), lit(0.0)) + coalesce(batchHolidayComparisonDF("holiday_sales"), lit(0.0))).alias("holiday_sales"),
        (coalesce(holidayComparisonMetricsDF("non_holiday_sales"), lit(0.0)) + coalesce(batchHolidayComparisonDF("non_holiday_sales"), lit(0.0))).alias("non_holiday_sales")
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
