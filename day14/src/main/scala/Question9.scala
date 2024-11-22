import org.apache.spark.sql.SparkSession

object Question9 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Question9")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val generateHundredNumbers = (1 to 100).toList
    val rdd = sc.parallelize(generateHundredNumbers)

    println(rdd.reduce(_+_))
//    println(rdd.sum())
    spark.stop()
  }
}
