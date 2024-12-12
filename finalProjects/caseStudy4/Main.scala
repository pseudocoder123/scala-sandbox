package caseStudy4

import caseStudy4.config.Config.{DataPaths, SparkConfig}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import dataProcessing.DataLoader.loadCSVData
import dataProcessing.DataSchemas
import dataProcessing.DataEnrich.validateAndEnrich
import dataStorage.DataStorage.{copyTrainDataToGCS, writeAggregatedMetrics, writeEnrichedData}
import metrics.MetricsProcessor.{computeDepartmentLevelMetrics, computeHolidaySalesComparison, computeStoreLevelMetrics, computeTopPerformingStores, computeWeeklyTrends}
import org.apache.spark.sql.functions.col

object Main {
  private val logger = LoggerFactory.getLogger(getClass)

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName(SparkConfig.appName)
      .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
      .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
      .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", DataPaths.gsServiceAccJsonPath)
      .config("spark.hadoop.fs.gs.auth.service.account.debug", "false")
      .master("local[*]")
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null

    try{
      spark = createSparkSession()
      spark.sparkContext.setLogLevel("WARN")
      logger.info("Spark Session is created")

      val trainDF = loadCSVData(spark, DataPaths.trainCSVPath, DataSchemas.getTrainSchema)  // Load the dataset
      // trainDF.show(10)

      val cachedTrainDF = trainDF.cache()

      /**
       * Validate and Enrich Data
       */
      val enrichedDF = validateAndEnrich(spark, cachedTrainDF)
      // enrichedDF.show(8)

      /**
       * Partition enrichedDF by Store and Date
       * Repartition shuffles the partitions and spark ensures that all rows with the same (store, date) combination are moved to the same memory partition.
       * Therefore, when partitionBy is used to physically write the data, the overhead of shuffling data will be minimised.
       * Also, any subsequent queries which filter on store and date will be optimised.
       */
      val repartitionedEnrichedDF = enrichedDF
        .repartition(col("store"), col("date"))
        .persist()  // Persist the dataframe so that there is no overhead for subsequent computations

      writeEnrichedData(repartitionedEnrichedDF) // write the enrichedData to the parquet format

      /**
       * Store-level metrics
       */
      val storeLevelMetrics = computeStoreLevelMetrics(repartitionedEnrichedDF)  // compute store-level metrics
      // storeLevelMetrics.show(8)
      val cachedStoreLevelMetrics = storeLevelMetrics.cache() // Cache can be used as the dataset is small

      computeTopPerformingStores(cachedStoreLevelMetrics)  // compute top performing stores
      writeAggregatedMetrics(cachedStoreLevelMetrics, "store")  // write Aggregated metrics

      /**
       * Department-level metrics
       */
      val departmentLevelMetrics = computeDepartmentLevelMetrics(repartitionedEnrichedDF)
      // departmentLevelMetrics.show(8)
      writeAggregatedMetrics(departmentLevelMetrics, "department")

      // Weekly trends
      val weeklyTrendsDF = computeWeeklyTrends(repartitionedEnrichedDF)
      // weeklyTrendsDF.show(8)
      writeAggregatedMetrics(weeklyTrendsDF, "weeklyTrends")

      // Holiday vs non-holiday sales
      val holidaySalesComparisonDF = computeHolidaySalesComparison(repartitionedEnrichedDF)
      // holidaySalesComparisonDF.show(8)
      writeAggregatedMetrics(holidaySalesComparisonDF, "holidaySalesComparison")

      /**
       * Real-Time Simulation: Support dynamic data ingestion.
       * Data Preparation: Copy existing train.csv data to gcs to support dynamic data ingestion [to facilitate ingestion into the pipeline]
       * KafkaProducer: The Kafka producer script is available under ingestion/KafkaMessageProducer.
       * SparkStreamConsumer: The Spark stream processing script is available under streaming/StreamProcessor.
       */
      copyTrainDataToGCS(cachedTrainDF)
      // println(spark.read.csv(DataPaths.gsWeeklySalesDataPath).count())

    }
    catch {
      case ex: Exception =>
        logger.error(s"Pipeline execution failed: ${ex.getMessage}")
        throw ex
    }
    finally {
      if(spark != null) spark.stop()
    }
  }
}
