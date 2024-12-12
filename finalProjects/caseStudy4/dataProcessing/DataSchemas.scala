package caseStudy4.dataProcessing

import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object DataSchemas {
  def getTrainSchema: StructType = StructType(Seq(
    StructField("store", IntegerType, nullable = true),
    StructField("department", IntegerType, nullable = true),
    StructField("date", DateType, nullable = true),
    StructField("weekly_sales", DoubleType, nullable = true),
    StructField("is_holiday", StringType, nullable = true)
  ))

  def getStoresSchema: StructType = StructType(Seq(
    StructField("store", IntegerType, nullable = true),
    StructField("type", StringType, nullable = true),
    StructField("size", IntegerType, nullable = true)
  ))

  def getFeaturesSchema: StructType = StructType(Seq(
    StructField("store", IntegerType, nullable = true),
    StructField("date", DateType, nullable = true),
    StructField("temperature", DoubleType, nullable = true),
    StructField("fuel_price", DoubleType, nullable = true),
    StructField("markdown1", DoubleType, nullable = true),
    StructField("markdown2", DoubleType, nullable = true),
    StructField("markdown3", DoubleType, nullable = true),
    StructField("markdown4", DoubleType, nullable = true),
    StructField("markdown5", DoubleType, nullable = true),
    StructField("cpi", DoubleType, nullable = true),
    StructField("unemployment", DoubleType, nullable = true),
    StructField("is_holiday", StringType, nullable = true)
  ))
}
