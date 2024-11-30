package Day18.Exercise5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object CreateUsersJson {
  def main(args: Array[String]): Unit = {
    val gsServiceAccJsonPath = "<PATH>/spark-gcs-key.json"

    val gsUserCsvPath = "gs://spark-tasks-bucket/day_18_19/exercise5/user_details"

    val spark = SparkSession.builder()
      .appName("User Details CSV To GCS")
      .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
      .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
      .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gsServiceAccJsonPath)
      .config("spark.hadoop.fs.gs.auth.service.account.debug", "true")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val schema = StructType(Seq(
      StructField("user_id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("email", StringType, nullable = false)

    ))

    val df = spark.read.option("header", "true").schema(schema).csv("<PATH>/user_details.csv")
    df.coalesce(1)
      .write
      .mode("overwrite")
      .parquet(gsUserCsvPath)


    spark.stop()

  }
}


