import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Question1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Question1")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val collection = Seq(
      "Sentence",
      "Two words",
      "Three word sentence",
      "Four words in sentence",
      "Five words in the sentence"
    )

    val rdd: RDD[String] = sc.parallelize(collection)

    val totalWords = rdd.flatMap(_.split(" ")).count()

    println(totalWords)

    spark.stop()
  }
}
