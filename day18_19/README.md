## Day18_19 Tasks

### Exercise-1
![](images/ex-1.png)

- Using broadcasting in Spark optimizes joins by sending the smaller dataset (user details) to all executors.
- This avoids expensive shuffling, enabling faster local joins with the larger dataset (transaction logs).
- Broadcasting reduces network overhead, improves performance, and is ideal when the smaller dataset fits in memory.
-----

### Exercise-2
![](images/ex-2-c.png)
![](images/ex-2-nc.png)
- Caching reduces execution time because the job does not re-execute all the RDD computations from the beginning.
- Instead, it reuses the cached RDD, avoiding redundant computations and reducing the processing time required to recreate the RDD.
--------

### Exercise-3
![](images/ex-3-1.png)
The kafka producer sends the messages as shown above.


![](images/ex-3-2.png)
![](images/ex-3-3.png)
The Spark Structured Streaming application reads these messages, extracts the required fields, and calculates the total amount in 10-second windows. The processed results are displayed in batches
----------

### Exercise-4
![](images/ex-4.png)

The input and output are successfully created in Parquet format in GCS, as expected.

-----------

### Exercise-5
![](images/ex-5-gcs.png)
- The enriched data is successfully written to the GCS path in JSON format.

The Kafka topic produces valid order messages that align with the schema (order_id, user_id, and amount).
![](images/ex-5-console.png)

#### Enrichment logic


- When user_id is found in the broadcasted user details dataset:
    - The enriched data contains the corresponding name, age, and email mapped from the broadcast dataset.

![](images/ex-5-result.png)

- When user_id is not found in the broadcasted dataset:
    - Default values are applied, resulting in the following mapping:<br/>
      - Name: "UNKNOWN"<br/>
      - Age: 0<br/>
      - Email: "NOT_AVAILABLE"<br/>
      These default values ensure completeness and consistency in the enriched data output.

![](images/ex-5-result-2.png)