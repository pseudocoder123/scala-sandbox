## Day18_19 Tasks

### Exercise-1
![](images/ex-1.png)

- Using broadcasting in Spark optimizes joins by sending the smaller dataset (user details) to all executors. 
- This avoids expensive shuffling, enabling faster local joins with the larger dataset (transaction logs). 
- Broadcasting reduces network overhead, improves performance, and is ideal when the smaller dataset fits in memory.

### Exercise-2
![](images/ex-2-c.png)
![](images/ex-2-nc.png)

- Caching reduces execution time because the job does not re-execute all the RDD computations from the beginning.
- Instead, it reuses the cached RDD, avoiding redundant computations and reducing the processing time required to recreate the RDD.

### Exercise-4
![](images/ex-4.png)

The input and output are successfully created in Parquet format in GCS, as expected.

