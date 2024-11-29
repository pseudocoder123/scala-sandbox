package Day18

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Exercise4 {
  def main(args: Array[String]): Unit = {
    val gsServiceAccJsonPath = "<PATH>/spark-gcs-key.json"

    val gsInputPath = "gs://spark-tasks-bucket/day_18_19/exercise4/input"
    val gsOutputPath = "gs://spark-tasks-bucket/day_18_19/exercise4/output"

    val spark = SparkSession.builder()
      .appName("JSON to CSV Conversion")
      .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
      .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
      .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gsServiceAccJsonPath)
      .config("spark.hadoop.fs.gs.auth.service.account.debug", "true")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext
    def generateData: Int => Seq[(Int, String)] = n => {
      (1 to n).map { ele =>
        if(ele%2 == 0) (ele, "IN_PROGRESS")
        else (ele, "COMPLETED")
      }
    }

    val dataRDD = sc.parallelize(generateData(100000))
    dataRDD.toDF("id", "status").coalesce(1).write.mode("overwrite").parquet(gsInputPath)

    // Read the data from GS
    val gsDF = spark.read.parquet(gsInputPath)

    // This will result both "IN_PROGRESS", "COMPLETED"
    gsDF.select("status").distinct().show()

    val filteredDF = gsDF.filter(col("status") === "COMPLETED")

    // This will result only "COMPLETED"
    filteredDF.select("status").distinct().show()

    filteredDF.coalesce(1)
      .write
      .mode("overwrite")
      .parquet(gsOutputPath)


    spark.stop()
  }
}
