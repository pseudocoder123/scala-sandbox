import org.apache.spark.sql.SparkSession

object Question5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Question5")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.parallelize(Seq((1, 23), (2, 45), (3, 56), (4, 49), (5, 78)))

    val result = rdd.map(_._2)
      .aggregate((0.0, 0))(
        (acc, score) => (acc._1 + score, acc._2 + 1),  // Local aggregation
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)  // Across partitions
      ) match {
      case (totalScore, count) => totalScore / count
    }

    println(result)
    spark.stop()
  }
}
