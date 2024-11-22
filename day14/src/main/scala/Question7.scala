import org.apache.spark.sql.SparkSession

object Question7 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Question7")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd1 = sc.parallelize(Seq(1,2,3,4,5))
    val rdd2 = sc.parallelize(Seq(4,5,6,7,8,9))

    val unionRDD = rdd1.union(rdd2)

    println(
      unionRDD.distinct()
      .collect()
      .mkString("Array(", ", ", ")")
    )

//    println(
//      unionRDD.map(ele => (ele, 1))
//        .countByKey()
//        .filter(_._2 == 1)
//        .keys
//        .mkString("Array(", ", ", ")")
//    )
    spark.stop()

  }
}
