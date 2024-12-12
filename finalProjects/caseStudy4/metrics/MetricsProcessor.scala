package caseStudy4.metrics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import caseStudy4.config.Config
import org.apache.spark.sql.expressions.Window

object MetricsProcessor {
  def computeStoreLevelMetrics(enrichedDF: DataFrame): DataFrame = {
   val storeLevelMetrics = enrichedDF
      .groupBy("store")
      .agg(
        sum("weekly_sales").alias("total_weekly_sales"),
        round(avg("weekly_sales"), 2).alias("average_weekly_sales"),
        count(lit(1)).alias("no_of_records")
      )
      .orderBy(col("total_weekly_sales").desc)

    // println(storeLevelMetrics.count())
    storeLevelMetrics
  }

  def computeTopPerformingStores(df: DataFrame): Unit = {
    df.limit(Config.noOfTopPerformingStores)
      .show()
  }

  def computeDepartmentLevelMetrics(df: DataFrame): DataFrame = {
    val departmentLevelMetrics = df
      .groupBy("store", "department")
      .agg(
        sum("weekly_sales").alias("total_weekly_sales")
      )

    // println(departmentLevelMetrics.count())
    departmentLevelMetrics
  }

  def computeWeeklyTrends(df: DataFrame): DataFrame = {
    val window = Window
      .partitionBy("store", "department") // Window that partitions the data by "store" and "department"
      .orderBy("date") // Orders the data within each partition by "date"

    val weeklyTrendsDF = df
      .select(
        col("store"),
        col("department"),
        col("date"),
        col("weekly_sales"),
        col("is_holiday"),
        lag("weekly_sales", 1).over(window).alias("previous_weekly_sales")
      )
      .withColumn("weekly_trend", col("weekly_sales") - col("previous_weekly_sales"))

    // weeklyTrendsDF.show()
    weeklyTrendsDF
  }

  def computeHolidaySalesComparison(df: DataFrame): DataFrame = {
    def computeSalesByHolidayStatus(df: DataFrame, holidayStatus: String, aliasName: String): DataFrame = {
      df.filter(col("is_holiday") === holidayStatus).groupBy("store", "department").agg(sum("weekly_sales").alias(aliasName))
    }

    val holidaySalesDF = computeSalesByHolidayStatus(df, "TRUE", "holiday_sales")
    val nonHolidaySalesDF = computeSalesByHolidayStatus(df, "FALSE", "non_holiday_sales")

    val joinedDF = holidaySalesDF.join(nonHolidaySalesDF, Seq("store", "department"), "outer")
      .orderBy(desc("holiday_sales"))

    // joinedDF.show()
    joinedDF
  }
}
