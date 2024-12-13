package caseStudy1.processing

import org.apache.spark.sql.types.{DateType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}

object DataSchema {
  def getSensorDataSchema: StructType = StructType(Seq(
    StructField("sensorId", IntegerType, nullable = true),
    StructField("timestamp", LongType, nullable = true),
    StructField("temperature", FloatType, nullable = true),
    StructField("humidity", FloatType, nullable = true)
  ))

  def getAggregatedSensorDataSchema: StructType = StructType(Seq(
    StructField("sensorId", IntegerType, nullable = true),
    StructField("averageTemperature", FloatType, nullable = true),
    StructField("averageHumidity", FloatType, nullable = true),
    StructField("minimumTemperature", FloatType, nullable = true),
    StructField("maximumTemperature", FloatType, nullable = true),
    StructField("minimumHumidity", FloatType, nullable = true),
    StructField("maximumHumidity", FloatType, nullable = true),
    StructField("noOfRecords", FloatType, nullable = true)
  ))
}
