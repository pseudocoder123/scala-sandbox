package Day18

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object Exercise2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Exercise2")
      .master("local[*]")
      .getOrCreate()

    val transactionDetailsDataPath = "<PATH>/transaction_logs_large.csv"

    val transactionLogsDF = spark.read
      .option("header", "true")
      .schema(StructType(Seq(
        StructField("transaction_id", IntegerType, nullable = false),
        StructField("user_id", IntegerType, nullable = false),
        StructField("amount", IntegerType, nullable = true)
      )))
      .csv(transactionDetailsDataPath)


    // Method to filter and aggregate DataFrame
    def processUsers(df: DataFrame, userType: String): DataFrame = {
      val condition = if (userType == "EVEN") col("user_id") % 2 === 0 else col("user_id") % 2 =!= 0
      df.filter(condition)
        .groupBy("user_id")
        .agg(sum("amount").alias("total_amount"))
    }

    // Method to process DataFrame with timing
    def measureExecutionTime(df: DataFrame, useCache: Boolean): Long = {
      if (useCache) df.cache()
      val startTime = System.currentTimeMillis()

      // Process both even and odd users
      val evenUsersDF = processUsers(df, "EVEN")
      val oddUsersDF = processUsers(df, "ODD")

      evenUsersDF.count() // Trigger action
      oddUsersDF.count()  // Trigger action

      val endTime = System.currentTimeMillis()
      if (useCache) df.unpersist()
      endTime - startTime
    }

    // Measure execution time for non-cached DataFrame
    val nonCachedTime = measureExecutionTime(transactionLogsDF, useCache = false)
    println(s"Non-cached execution time: $nonCachedTime ms")

    // Measure execution time for cached DataFrame
    val cachedTime = measureExecutionTime(transactionLogsDF, useCache = true)
    println(s"Cached execution time: $cachedTime ms")

    spark.stop()
  }
}