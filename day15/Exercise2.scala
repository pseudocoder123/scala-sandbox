package Day15.exercises

import org.apache.spark.sql.SparkSession

object Exercise2{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Exercise2 - Narrow and Wide transformations")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd = sc.parallelize(1 to 1000)

    val mapRDD = rdd.map(ele => (ele, ele*2))
    val filterRDD = mapRDD.filter{ case (key, _) => key % 2 == 0 }

    val groupedRDD = filterRDD.groupByKey()
    groupedRDD.saveAsTextFile("exercise21.txt")

    val reducedRDD = filterRDD.reduceByKey(_+_)
    reducedRDD.saveAsTextFile("exercise22.txt")

    // Hold the Spark UI
    println("Application is running. Press Enter to exit.")
    scala.io.StdIn.readLine()

    // Stop the SparkContext
    sc.stop()
  }
}
