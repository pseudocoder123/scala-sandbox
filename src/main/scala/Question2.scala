import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Question2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Double Numbers")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val list1 = List(1,2,3)
    val list2 = List(4,5,6)

    val parallelize: List[Int] => RDD[Int] = l => sc.parallelize(l)

    val (rdd1, rdd2) = (parallelize(list1), parallelize(list2))
    println(rdd1.cartesian(rdd2).collect().mkString("Array(", ", ", ")"))
  }
}
