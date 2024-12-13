package caseStudy1.test

import caseStudy1.processing.ProcessIncrementalAggregation.getBatchAggregatedDF
import org.apache.spark.sql.SparkSession

// NOTE: Should be inside the test folder generated

class SampleTest extends AnyFunSuite {
  val spark: SparkSession = SparkSession.builder()
    .appName("Sample Test: Generating Batch Aggregate DF")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("generateBatchAggregateDF should aggregate with respect to sensorId and return the correct results") {
    // Prepare sample data
    val sensorData = Seq(
      (1, 1733960468159L, 120.46, 80.76),
      (1, 1733960468159L, 56.78, 34.56),
      (2, 1733960468159L, 78.68, 54.32),
      (1, 1733960468159L, -32.56, 23.49),
      (2, 1733960468159L, 0.0, 0.0)
    ).toDF("sensorId", "timestamp", "temperature", "humidity")

    // Perform transformation
    val result = getBatchAggregatedDF(sensorData)

    // Expected DataFrame
    val expectedData = Seq(
      (1, 48.226665f, 46.27f, -32.56f, 120.46f, 23.49f, 80.76f, 3L),
      (2, 39.34f, 27.16f, 0.0f, 78.68f, 0.0f, 54.32f, 2L)
    ).toDF("sensorId", "averageTemperature", "averageHumidity", "minimumTemperature", "maximumTemperature", "minimumHumidity", "maximumHumidity", "noOfRecords")

    // Assertion to check if the transformation result matches expected result
    assert(result.collect().mkString("Array(", ",", ")") == expectedData.collect().mkString("Array(", ",", ")"), "The results do not match the expected output.")
  }
}
