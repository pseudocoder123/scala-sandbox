package Day18.Exercise5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object GetEnrichedData {
  def main(args: Array[String]): Unit = {
    val gsServiceAccJsonPath = "<PATH>/spark-gcs-key.json"
    val path = "gs://spark-tasks-bucket/day_18_19/exercise5/enriched_data"

    val spark = SparkSession.builder()
      .appName("JSON to CSV Conversion")
      .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
      .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
      .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gsServiceAccJsonPath)
      .config("spark.hadoop.fs.gs.auth.service.account.debug", "true")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read.json(path)
    df.show()

    // Get the userId absence data
    df.filter(col("name") === "UNKNOWN" && col("age") === 0 && col("email") === "NOT_AVAILABLE")
      .show()

    spark.stop()

  }
}
