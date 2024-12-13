## Real-Time Simulation

### Architectural Diagram
![](images/architecture.png)

### Compilation and Descriptor Generation Commands:
To generate the compiled files and descriptor files from the .proto file, the following commands are used:
- Generate Compiled Files:
    - SensorReading: ```protoc --descriptor_set_out=src/main/scala/caseStudy1/protobuf/descriptor/SensorReading.desc --include_imports --proto_path=src/main/scala/caseStudy1/protobuf/proto src/main/scala/caseStudy1/protobuf/proto/SensorReading.proto```
    - AggregatedSensorReading: ```protoc --descriptor_set_out=src/main/scala/caseStudy1/protobuf/descriptor/AggregatedSensorReading.desc --include_imports --proto_path=src/main/scala/caseStudy1/protobuf/proto src/main/scala/caseStudy1/protobuf/proto/AggregatedSensorReading.proto```
- Generate Descriptor Files:
  - SensorReading: ```protoc --scala_out=src/main/scala --proto_path=src/main/scala/caseStudy1/protobuf/proto src/main/scala/caseStudy1/protobuf/proto/SensorReading.proto```
  - AggregatedSensorReading```protoc --scala_out=src/main/scala --proto_path=src/main/scala/caseStudy1/protobuf/proto src/main/scala/caseStudy1/protobuf/proto/AggregatedSensorReading.proto```

### Real-Time Data Simulation with Protobuf and Kafka:
- Akka SensorReadings Producer [Produced to Kafka]:

![](images/sensorAkkaProducer.png)

- Once serialized into binary format, the data produced to Kafka appears as:

![](images/sensorKafkaConsole.png)

The Akka producer has successfully produced 1707 messages, all of which have been received by Kafka, ensuring no data loss. 

### Data Processing with Spark Streaming and GCP:
- The serialized data from Kafka is consumed by a Spark Streaming job.

![](images/sparkSensorReadings.png)

- Stored to GCP based on eventDate

![](images/eventTimeBasedRawDataGCP.png)

- Stored to GCP based on processingTime

![](images/processingTimeBasedRawDataGCP.png)

- Aggregated metrics are calculated incrementally using the data from kafka and previous aggregated results

![](images/aggregatedMetrics.png)

- Aggregated metrics are stored to GCP in protobuf format

![](images/aggregatedMetricsGCP.png)

- Aggregated metrics are also stored in JSON format to GCP in JSON format

![](images/aggregatedJSONGCP.png)

### APIs:

- Aggregated Data API:

![](images/aggDataAPI.png)

- Aggregated Data API for a particular sensorId (sensorId = 1)

![](images/sensorIdAPI.png)

### UI:
Used streamlit for UI

- Aggregated Data API called on UI

![](images/AggUI_1.png)
![](images/AggUI_2.png)

- Aggregated Data API for particular sensorId (sensorId=1) on UI

![](images/aggSensorIDUI.png)

### Retention:

The provided logic demonstrates how to identify a folder in GCP that belongs to the current day and is not older than seven days, as outlined in the question. This folder can then be accessed as needed.
Similarly, the logic has been modified to get the target folders older than seven days, which can subsequently be deleted from the file system.

The corresponding implementation can be found [here](retention/RetentionPolicy.scala)
![](images/RetentionPolicy.png)

### Test cases:
![](images/sampleTest.png)


