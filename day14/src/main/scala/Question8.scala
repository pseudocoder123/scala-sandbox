import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Question8 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Question8")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    case class CSVRecords(sno: Int, name: String, age: Int)
    implicit def convertToCSVRecords(str: String): CSVRecords = {
      val strList = str.split(",")
      CSVRecords(strList.head.toInt, strList(1), strList.last.toInt)
    }

    val rdd: RDD[CSVRecords] = sc.parallelize(Seq(
      "1,Saketh,24",
      "2,John,45",
      "3,Albert,56",
      "4,Malkovich,13",
      "5,Anna,18",
      "6,Ben,5",
      "7,Charlie,6"
    ))

    val result = rdd.filter(_.age < 18)
      .map(ele => s"Name: ${ele.name} with Age: ${ele.age}")
      .collect()
    println(result.mkString("Array(", ", ", ")"))
    spark.stop()
  }
}
