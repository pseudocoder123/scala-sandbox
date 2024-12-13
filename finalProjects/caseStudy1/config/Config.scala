package caseStudy1.config

object Config {
  val retentionPeriod = 1

  object SparkConfig {
    val sensorDataStreamAppName: String = "SensorDataStreamReader"
    val retentionPolicyAppName: String = "RetentionPolicy"
  }

  object ProtobufMessageType {
    val sensorDataMessageType: String = "caseStudy1.protobuf.SensorReading"   // Fully qualified protobuf type
    val aggregatedMessageType: String = "caseStudy1.protobuf.AggregatedSensorReading"
  }

  object DataPaths {
    val gsDefaultFS: String = "gs://spark-tasks-bucket/"
    val gsServiceAccJsonPath: String = "/Users/sakethmuthoju/Desktop/spark-gcs-key.json"

    val gsEventTimeBasedRawDataPath = "gs://spark-tasks-bucket/final-projects/caseStudy1/event_time/raw/sensor-data"
    val gsProcessingTimeBasedRawDataParentPath = "gs://spark-tasks-bucket/final-projects/caseStudy1/raw/sensor-data"
    val gsAggregationDataParentPath = "gs://spark-tasks-bucket/final-projects/caseStudy1/aggregated/protobuf"
    val gsAggregationJsonDataParentPath = "gs://spark-tasks-bucket/final-projects/caseStudy1/aggregated/json"

    val rawDataProtoDescriptorPath = "src/main/scala/caseStudy1/protobuf/descriptor/SensorReading.desc"
    val aggregatedDataProtoDescriptorPath = "src/main/scala/caseStudy1/protobuf/descriptor/AggregatedSensorReading.desc"
  }

  object KafkaConfig {
    val bootstrapServers: String = "localhost:9092"
    val topic: String = "sensor-readings"
  }
}
