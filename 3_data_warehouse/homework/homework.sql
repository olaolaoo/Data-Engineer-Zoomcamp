-- Create an external table using the Green Taxi Trip Records Data for 2022.
CREATE OR REPLACE EXTERNAL TABLE `coherent-ascent-379901.ny_taxi.external_green_tripdata`
OPTIONS(
    format = 'parquet',
    uris = ['gs://dtc-zoomcamp/green_tripdata_2022-*.parquet']
);

-- Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).
CREATE OR REPLACE TABLE coherent-ascent-379901.ny_taxi.green_tripdata_non_partitioned AS
SELECT * FROM coherent-ascent-379901.ny_taxi.external_green_tripdata;

-- Question 1: What is count of records for the 2022 Green Taxi Data??
--Answer: 840402
SELECT COUNT(1) FROM coherent-ascent-379901.ny_taxi.green_tripdata_non_partitioned;

-- Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
--Answer: 0 MB for the External Table and 6.41MB for the Materialized Table, I got the same number, but it's not in the options.
--6.41 MB
SELECT DISTINCT(PULocationID) AS all_PULocationID
FROM coherent-ascent-379901.ny_taxi.external_green_tripdata;
--6.41 MB
SELECT DISTINCT(PULocationID) AS all_PULocationID
FROM coherent-ascent-379901.ny_taxi.green_tripdata_non_partitioned;

-- Question 3: How many records have a fare_amount of 0?
--Answer: 1622
SELECT COUNT(1)
FROM coherent-ascent-379901.ny_taxi.green_tripdata_non_partitioned
WHERE fare_amount=0;

-- Question 4: What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
--Answer: Partition by lpep_pickup_datetime Cluster on PUlocationID
CREATE OR REPLACE TABLE coherent-ascent-379901.ny_taxi.green_tripdata_partitioned_clusterd 
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM coherent-ascent-379901.ny_taxi.green_tripdata_non_partitioned

-- Question 5: Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
-- Use the materialized table you created earlier in your from clause and note the estimated bytes. 
--Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?
--Answer: 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table
--12.82 MB
SELECT DISTINCT(PULocationID) AS all_PULocationID
FROM coherent-ascent-379901.ny_taxi.green_tripdata_non_partitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
--1.12 MB
SELECT DISTINCT(PULocationID) AS all_PULocationID
FROM coherent-ascent-379901.ny_taxi.green_tripdata_partitioned_clusterd
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- Question 8: 
SELECT *
FROM coherent-ascent-379901.ny_taxi.green_tripdata_non_partitioned





