package caseStudy4.config

object Config {
  val noOfTopPerformingStores = 3

  object SparkConfig {
    val appName: String = "caseStudy4"
    val salesStreamAppName: String = "salesStreamReader"
  }

  object DataPaths {
    val trainCSVPath: String = "src/main/scala/CaseStudy4/resources/train.csv"
    val featuresCSVPath: String = "src/main/scala/CaseStudy4/resources/features.csv"
    val storesCSVPath: String = "src/main/scala/CaseStudy4/resources/stores.csv"

    val gsServiceAccJsonPath: String = "/Users/sakethmuthoju/Desktop/spark-gcs-key.json"

    val gsEnrichedDataOutputPath = "gs://spark-tasks-bucket/final-projects/caseStudy4/enriched_data"
    val gsAggregatedStoreMetrics = "gs://spark-tasks-bucket/final-projects/caseStudy4/aggregated_metrics/store_level"
    val gsAggregatedDepartmentMetrics = "gs://spark-tasks-bucket/final-projects/caseStudy4/aggregated_metrics/department_level"
    val gsAggregatedWeeklyTrendsMetrics = "gs://spark-tasks-bucket/final-projects/caseStudy4/aggregated_metrics/weekly_trends"
    val gsAggregatedHolidayComparisonSalesMetrics = "gs://spark-tasks-bucket/final-projects/caseStudy4/aggregated_metrics/holiday_comparison_sales"

    val gsWeeklySalesDataPath = "gs://spark-tasks-bucket/final-projects/caseStudy4/weekly_sales_data"

    val protoDescriptorPath = "src/main/scala/caseStudy4/protobuf/descriptor/WeeklySalesData.desc"
  }

  object KafkaConfig {
    val bootstrapServers: String = "localhost:9092"
    val topic: String = "walmart-recruiting"
  }
}
