import org.apache.spark.sql.SparkSession

object Question10 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Double Numbers")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.parallelize(Seq(
      (1, 10), (2, 20), (1, 10), (2, 20), (3, 30), (4, 70), (3, 10)
    ))

    val result = rdd.reduceByKey(_+_).collect()
    println(result.mkString("Array(", ", ", ")"))
  }
}
