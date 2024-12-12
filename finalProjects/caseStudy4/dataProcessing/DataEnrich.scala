package caseStudy4.dataProcessing

import caseStudy4.config.Config.DataPaths
import caseStudy4.dataProcessing.DataLoader.loadCSVData
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._

object DataEnrich {
  private val logger = LoggerFactory.getLogger(getClass)

  def validateAndEnrich(spark: SparkSession, trainDF: DataFrame): DataFrame = {
    try{
      val (storesDF, featuresDF) = loadStaticDatasets(spark)
      // println(storesDF.filter(col("store").isNull || col("type").isNull || col("size").isNull).count()) // Shows no invalid records
      // println(featuresDF.filter(col("store").isNull || col("date").isNull).count()) // Shows no invalid records

      // println(trainDF.filter(col("weekly_sales") < 0).count())
      val updatedTrainDF = trainDF
        .filter(col("weekly_sales") >= 0) // Filter out records where weekly_sales is negative
        .filter(                          // Filtering invalid rows
          col("store").isNotNull && col("store") > 0 &&
            col("department").isNotNull && col("department") > 0 &&
            col("date").isNotNull
        )
        .withColumn("weekly_sales", when(col("weekly_sales").isNull, lit(0.0)).otherwise(col("weekly_sales"))) // weekly_sales is null, default to 0.0
      // println(updatedTrainDF.count())

      // Broadcast small datasets for join optimisation
      val storesBroadcast = broadcast(storesDF)
      val featuresBroadcast = broadcast(featuresDF)

      val enrichedDF = updatedTrainDF
        .join(storesBroadcast, Seq("store"), "left")
        .join(featuresBroadcast, Seq("store", "date", "is_holiday"), "left")

      logger.info("Data enrichment completed")
      enrichedDF
    }
    catch {
      case ex: Exception =>
        logger.error(s"Error in data validation and enrichment: ${ex.getMessage}")
        throw ex
    }
  }

  // Load the static datasets: `stores.csv` and `features.csv`
  private def loadStaticDatasets(spark: SparkSession): (DataFrame, DataFrame) = {
    val storesDF = loadCSVData(spark, DataPaths.storesCSVPath, DataSchemas.getStoresSchema)
    val featuresDF = loadCSVData(spark, DataPaths.featuresCSVPath, DataSchemas.getFeaturesSchema)

    // storesDF.show(10)
    // featuresDF.show(10)

    (storesDF, featuresDF)
  }
}
