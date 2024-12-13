package caseStudy1.apiController

import caseStudy1.config.Config.DataPaths
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.util.{Failure, Success, Try}

object MetricsControllerServiceImpl {
  def fetchAggregatedMetrics(spark: SparkSession): String = {
    getLatestAggregatedGCPFolder(spark) match {
      case Success(fp) =>
        Try(spark.read.json(fp)) match {
          case Success(df) => df.toJSON.collect().mkString("[", ",", "]")
          case Failure(_) => "[]"
        }
      case Failure(_) => "[]"
    }
  }

  def fetchAggregatedMetricsBySensorId(spark: SparkSession, sensorId: Int): String = {
    getLatestAggregatedGCPFolder(spark) match {
      case Success(fp) =>
        Try {
          spark.read.json(fp)
            .filter(col("sensorId") === sensorId)
            .toJSON
            .collect()
            .mkString("[", ",", "]")
        }.getOrElse("[]")
      case Failure(_) => "[]"
    }
  }

  def getLatestAggregatedGCPFolder(spark: SparkSession): Try[String] = Try {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val basePathObj = new Path(DataPaths.gsAggregationJsonDataParentPath)

    require(fs.exists(basePathObj), "Base path does not exist") // Fail fast if base path doesn't exist

    def findLatestFolder(path: Path): Path = {
      val status = fs.listStatus(path)
      require(status.nonEmpty, s"No subdirectories found in $path")

      status
        .filter(_.isDirectory)
        .map(_.getPath)
        .maxBy(_.getName.toInt)
    }

    val latestPath = List(
      basePathObj,
      findLatestFolder(basePathObj),
      findLatestFolder(findLatestFolder(basePathObj)),
      findLatestFolder(findLatestFolder(findLatestFolder(basePathObj))),
      findLatestFolder(findLatestFolder(findLatestFolder(findLatestFolder(basePathObj))))
    ).last

    latestPath.toString
  }

//  def getLatestAggregatedGCPFolder(spark: SparkSession): Option[String] = {
//    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
//    val basePathObj = new Path(DataPaths.gsAggregationJsonDataParentPath)
//
//    if (!fs.exists(basePathObj)) return None
//
//    def maxByOption[T](seq: Seq[T])(compare: T => Int): Option[T] = {
//      if (seq.isEmpty) None
//      else Some(seq.maxBy(compare))
//    }
//
//    try {
//      def getMaxFolder(path: Path): Option[Path] = {
//        val status = fs.listStatus(path)
//        if (status.isEmpty) None
//        else {
//          val directories = status.filter(_.isDirectory).map(_.getPath)
//          maxByOption(directories)(_.getName.toInt)
//        }
//      }
//
//      val maxYear = getMaxFolder(basePathObj)
//      val maxMonth = maxYear.flatMap(getMaxFolder)
//      val maxDay = maxMonth.flatMap(getMaxFolder)
//      val maxHour = maxDay.flatMap(getMaxFolder)
//
//      maxHour.map(_.toString)
//    } catch {
//      case e: Exception =>
//        println(s"Error while finding the latest folder: ${e.getMessage}")
//        None
//    }
//  }
}
