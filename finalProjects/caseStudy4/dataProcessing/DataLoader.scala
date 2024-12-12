package caseStudy4.dataProcessing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

object DataLoader {
  private val logger = LoggerFactory.getLogger(getClass)

  def loadCSVData(spark: SparkSession, path: String, schema: StructType, header: String = "true", cache: Boolean = false): DataFrame = {
    try {
      val df = spark.read
        .schema(schema)
        .option("header", header)
        .csv(path)

      if(cache) {
        df.cache()
      }

      if(cache) logger.info(s"Dataset from $path loaded and cached") else logger.info(s"Dataset from $path loaded")

      df
    }
    catch {
      case ex: Exception =>
        logger.error(s"Error loading dataset from $path: ${ex.getMessage}")
        throw ex
    }

  }
}
