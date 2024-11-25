package Day15.exercises

import org.apache.spark.sql.SparkSession

object Exercise3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Exercise3 - Analyzing Tasks and Executors")
      .master("local[*]")
      .config("spark.executor.instances", "2")
      .getOrCreate()

    val sc = spark.sparkContext

    def createMillionLines: Seq[String] = {
      val lines = Seq(
        "One",
        "Two words",
        "Three words present",
        "Four words in sentence",
        "Five words are being included"
      )

      lines.flatMap(Seq.fill(200000)(_))
    }

    val rdd = sc.parallelize(createMillionLines, numSlices = 6)
      .flatMap(_.split(" "))
      .map(_.toLowerCase)
      .map(ele => (ele, 1))
      .reduceByKey(_+_)
      .collect()

    println(rdd.mkString("Array(",",",")"))

    // Hold the Spark UI
    println("Application is running. Press Enter to exit.")
    scala.io.StdIn.readLine()

    // Stop the SparkContext
    sc.stop()
  }
}
