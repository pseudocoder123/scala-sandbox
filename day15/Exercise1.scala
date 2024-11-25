package Day15.exercises

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io._

object Exercise1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Exercise1 - Understanding RDD and Partitioning")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val fp = "exercise1.txt"

    // Write to file
    def writeToFile(fp: String, numRecords: Int): Unit = {
      val rnd = new scala.util.Random
      val bw = new BufferedWriter(new FileWriter(fp))
      for (_ <- 1 to numRecords) {
        bw.write((1 + rnd.nextInt(numRecords)).toString + "\n")
      }
      bw.close()
    }

    writeToFile(fp, 10000000)

    // Load the file
    val rdd: RDD[String] = sc.textFile(fp)
    println(s"Number of partitions of rdd are: ${rdd.getNumPartitions}")
    rdd.foreachPartition(partition =>
      println(s"Number of elements in paritition: ${partition.size}")
    )

    val repartitionedRDD = rdd.repartition(4)
    println(s"Number of partitions of repartitionedRDD are: ${repartitionedRDD.getNumPartitions}")
    repartitionedRDD.foreachPartition(partition =>
      println(s"Number of elements in paritition: ${partition.size}")
    )

    val coalesceRDD = repartitionedRDD.coalesce(2)
    println(s"Number of partitions of coalesceRDD are: ${coalesceRDD.getNumPartitions}")
    coalesceRDD.foreachPartition(partition =>
      println(s"Number of elements in paritition: ${partition.size}")
    )

    def printFirstFiveElementsFromPartitions(rdd: RDD[String]): Unit = {
      rdd.mapPartitionsWithIndex { (idx, iter) =>
        Iterator((idx, iter.take(5).toList))
      }.collect().foreach { case (partitionIndex, elements) =>
        println(s"Partition $partitionIndex: ${elements.mkString(", ")}")
      }
    }
    printFirstFiveElementsFromPartitions(coalesceRDD)

    // Hold the Spark UI
    println("Application is running. Press Enter to exit.")
    scala.io.StdIn.readLine()

    // Stop the sparkContext
    sc.stop()
  }
}
