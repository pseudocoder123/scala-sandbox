package Day15.exercises

import org.apache.spark.sql.SparkSession

object Exercise4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Exercise4 - Exploring DAG and Spark UI")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd = sc.parallelize(1 to 10000)
      .filter(_%2 == 0)
      .map(_*10)
      .flatMap(ele => Seq((ele, ele+1)))
      .reduceByKey(_+_)

    val result = rdd.collect()

    result.take(5).foreach { case(key, value) =>
      println(s"Key: $key and Value: $value")
    }

    // Hold the Spark UI
    println("Application is running. Press Enter to exit.")
    scala.io.StdIn.readLine()

    // Stop the SparkContext
    sc.stop()

  }
}
