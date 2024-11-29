package Day18

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Exercise1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Exercise1")
      .master("local[*]")
      .getOrCreate()

    val userDetailsDataPath = "<PATH>/user_details.csv"
    val transactionDetailsDataPath = "<PATH>/transaction_logs_large.csv"

    // Create userDetailsDF and transactionLogsDF
    val userDetailsDF = spark.read
      .option("header", "true")
      .schema(StructType(Seq(
        StructField("user_id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = true)
      )))
      .csv(userDetailsDataPath)

    val transactionLogsDF = spark.read
      .option("header", "true")
      .schema(StructType(Seq(
        StructField("transaction_id", IntegerType, nullable = false),
        StructField("user_id", IntegerType, nullable = false),
        StructField("amount", IntegerType, nullable = true)
      )))
      .csv(transactionDetailsDataPath)

    // Join the operations without broadcast
    val nonBroadcastStartTime = System.currentTimeMillis()
    val nonBroadcastJoinedDF = userDetailsDF.join(transactionLogsDF, "user_id")
    println(nonBroadcastJoinedDF.count())
    val nonBroadcastEndTime = System.currentTimeMillis()

    // Join the operations with broadcast
    val broadcastStartTime = System.currentTimeMillis()
    val broadcastJoinedDF = userDetailsDF.join(broadcast(transactionLogsDF), "user_id")
    println(broadcastJoinedDF.count())
    val broadcastEndTime = System.currentTimeMillis()

    println(s"Non-Broadcast Join Time: ${nonBroadcastEndTime - nonBroadcastStartTime} ms")
    println(s"Broadcast Join Time: ${broadcastEndTime - broadcastStartTime} ms")


    spark.stop()
  }
}
