package caseStudy1.retention

import caseStudy1.config.Config
import caseStudy1.config.Config.{DataPaths, SparkConfig}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

object RetentionPolicy {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(SparkConfig.retentionPolicyAppName)
      .config("spark.hadoop.fs.defaultFS", DataPaths.gsDefaultFS)
      .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
      .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
      .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", DataPaths.gsServiceAccJsonPath)
      .config("spark.hadoop.fs.gs.auth.service.account.debug", "false")
      .master("local[*]")
      .getOrCreate()

    val formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH")
    val cutOffTime = LocalDateTime.now().minus(Config.retentionPeriod, ChronoUnit.DAYS)

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val yearPaths = fs.listStatus(new Path(DataPaths.gsProcessingTimeBasedRawDataParentPath)).map(_.getPath)
    deleteOldFolders(yearPaths, fs, cutOffTime, formatter)

    spark.stop()
  }

  def deleteOldFolders(yearPaths: Seq[Path], fs: FileSystem, cutOffTime: LocalDateTime, formatter: DateTimeFormatter) = {
    val paths = yearPaths
      .iterator
      .flatMap(yearPath =>
        fs.listStatus(yearPath)
          .iterator
          .flatMap(monthPath =>
            fs.listStatus(monthPath.getPath)
              .iterator
              .flatMap(dayPath =>
                fs.listStatus(dayPath.getPath)
                  .iterator
                  .map(hourPath => (yearPath, monthPath, dayPath, hourPath))
              )
          )
      )

//      paths.map { case (yearPath, monthPath, dayPath, hourPath) => s"${yearPath.getName}/${monthPath.getPath.getName}/${dayPath.getPath.getName}/${hourPath.getPath.getName}" }
//      .foreach(println)

      paths.filter { case (yearPath, monthPath, dayPath, hourPath) =>
        val fullPath = s"${yearPath.getName}/${monthPath.getPath.getName}/${dayPath.getPath.getName}/${hourPath.getPath.getName}"
        val folderTimestamp = LocalDateTime.parse(fullPath, formatter)
        folderTimestamp.isBefore(cutOffTime)
//        folderTimestamp.isAfter(cutOffTime)
      }
      .foreach { case (_, _, _, hourPath) =>
        val path = hourPath.getPath
//        println(path)
        fs.delete(path, true)
      }
  }

}
