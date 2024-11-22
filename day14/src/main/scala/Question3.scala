import org.apache.spark.sql.SparkSession

object Question3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Question3")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.parallelize(Seq(1,2,3,4,5,6,7,8,9))

    println(rdd.filter(_%2 == 0).collect().mkString("Array(", ",", ")"))

    spark.stop()
  }
}
