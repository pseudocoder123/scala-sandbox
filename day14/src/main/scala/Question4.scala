import org.apache.spark.sql.SparkSession

object Question4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Double Numbers")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.parallelize(Seq("This", "is", "a", "well", "written sentence"))
    val result = rdd.flatMap(_.toLowerCase)
      .filter(_ != ' ')
      .map((_, 1))
      .reduceByKey(_+_)
      .collect()

    println(result.mkString("Array(", ", ", ")"))
  }
}
