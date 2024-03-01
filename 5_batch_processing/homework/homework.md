## Week 5 Homework

[Homework](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2024/05-batch/homework.md)

## Question 1. **Execute spark.version.** 

> **Answer: '3.5.0'**

## Question 2.FHV October 2019 after 6 partitions ,average size of the parque

> **Answer:`6mb`**

Read the data and run `spark.createDataFrame(df_pandas).schema` to get the schema. Then modify it and run `df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('fhv_tripdata_2019-10.csv')`

the output is:

```bash
(base) ola@192 2019 % ls -lh 
total 74496
-rw-r--r--@ 1 ola  staff     0B  3  1 20:34 _SUCCESS
-rw-r--r--@ 1 ola  staff   6.0M  3  1 20:34 part-00000-f9ebd481-6a5a-490d-8926-8d7578b44671-c000.snappy.parquet
-rw-r--r--@ 1 ola  staff   6.0M  3  1 20:34 part-00001-f9ebd481-6a5a-490d-8926-8d7578b44671-c000.snappy.parquet
-rw-r--r--@ 1 ola  staff   6.0M  3  1 20:34 part-00002-f9ebd481-6a5a-490d-8926-8d7578b44671-c000.snappy.parquet
-rw-r--r--@ 1 ola  staff   6.0M  3  1 20:34 part-00003-f9ebd481-6a5a-490d-8926-8d7578b44671-c000.snappy.parquet
-rw-r--r--@ 1 ola  staff   6.0M  3  1 20:34 part-00004-f9ebd481-6a5a-490d-8926-8d7578b44671-c000.snappy.parquet
-rw-r--r--@ 1 ola  staff   6.0M  3  1 20:34 part-00005-f9ebd481-6a5a-490d-8926-8d7578b44671-c000.snappy.parquet
```



## Question 3. How many taxi trips were there on the 15th of October?

> **Answer: 62610**

Run the code:

```sql
df.createOrReplaceTempView('trip_data')


spark.sql("""
SELECT
    count(1)
FROM
    trip_data
WHERE
    date_format(pickup_datetime, 'yyyy-MM-dd') = '2019-10-15'
""").show()
```

Output:

```python
[Stage 14:>                                                         (0 + 8) / 8]
+--------+
|count(1)|
+--------+
|   62610|
+--------+
```

## Question 4. What is the length of the longest trip in the dataset in hours?

> **Answer: 631,152.50 Hours**

```sql
spark.sql("""
SELECT 
        date_format(pickup_datetime, 'yyyy-MM-dd') AS day,
        (unix_timestamp(dropOff_datetime)-unix_timestamp(pickup_datetime))/(60)/(60) as diff
FROM trip_data
ORDER BY diff DESC
LIMIT 1
""").show()
```

```python
[Stage 37:>                                                         (0 + 8) / 8]
+----------+--------+
|       day|    diff|
+----------+--------+
|2019-10-28|631152.5|
+----------+--------+
```

## Question 5. Spark’s User Interface which shows the application's dashboard runs on which local port?

> **Answer: 4040**

![](../../notes/images/hw_05.png)

## Question 6. What is the length of the longest trip in the dataset in hours?

> **Answer: Jamaica Bay**

Run:

```python
df_zone_s = spark.read \
    .option("header", "true") \
    .csv('taxi_zone_lookup.csv')

df_zone_s.createOrReplaceTempView('zone')

spark.sql("""
    SELECT PUlocationID,Zone,
            count(1) AS trips  
    FROM trip_data
    LEFT JOIN zone
    ON trip_data.PUlocationID=zone.LocationID
    GROUP BY 1,2
    ORDER BY trips
    LIMIT 5
""").show()
```

Output:

```
[Stage 41:=======>                                                  (1 + 7) / 8]
+------------+--------------------+-----+
|PUlocationID|                Zone|trips|
+------------+--------------------+-----+
|           2|         Jamaica Bay|    1|
|         105|Governor's Island...|    2|
|         111| Green-Wood Cemetery|    5|
|          30|       Broad Channel|    8|
|         120|     Highbridge Park|   14|
+------------+--------------------+-----+
```

## Submitting the solutions 

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw5

Due date: March 4, 2024, 11 p.m.