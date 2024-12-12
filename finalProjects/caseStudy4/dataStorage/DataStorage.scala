package caseStudy4.dataStorage

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory
import caseStudy4.config.Config.DataPaths

object DataStorage {
  private val logger = LoggerFactory.getLogger(getClass)

  def writeEnrichedData(enrichedDF: DataFrame): Unit = {
    try{
      enrichedDF
        .limit(1000)
        .write
        .partitionBy("store", "date")
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .save(DataPaths.gsEnrichedDataOutputPath)
    }
    catch {
      case ex: Exception =>
        logger.error(s"Error in storing enriched data in GCS: ${ex.getMessage}")
        throw ex
    }
  }

  def writeAggregatedMetrics(df: DataFrame, entity: String): Unit = {
    try {
      val path = entity match {
        case "store" => DataPaths.gsAggregatedStoreMetrics
        case "department" => DataPaths.gsAggregatedDepartmentMetrics
        case "weeklyTrends" => DataPaths.gsAggregatedWeeklyTrendsMetrics
        case "holidaySalesComparison" => DataPaths.gsAggregatedHolidayComparisonSalesMetrics
        case _ => throw new Exception("Aggregated entity is not defined")
      }

      val dfToWrite = if(entity.equals("weeklyTrends")) df.limit(1000) else df

      dfToWrite
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .format("json")
        .save(path)
    }
    catch {
      case ex: Exception =>
        logger.error(s"Error in storing aggregated store data in GCS: ${ex.getMessage}")
        throw ex
    }
  }

  def copyTrainDataToGCS(trainDF: DataFrame): Unit = {
    trainDF
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .save(DataPaths.gsWeeklySalesDataPath)
  }
}
