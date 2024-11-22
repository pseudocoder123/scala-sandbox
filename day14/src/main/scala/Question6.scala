import org.apache.spark.sql.SparkSession

object Question6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Question6")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd1 = sc.parallelize(Seq(
      (1, "Saketh"),
      (2, "Alice"),
      (3, "Bob"),
      (4, "Charlie"),
      (5, "Donald")
    ))

    val rdd2 = sc.parallelize(Seq(
      (1, 100),
      (2, 50),
      (3, 65),
      (4, 78),
      (5, 89)
    ))

    val result = rdd1.join(rdd2)
      .map{
        case (id, (name, score)) => (id, name, score)
      }
      .collect()

    println(result.mkString("Array(", ", ", ")"))
    spark.stop()
  }
}