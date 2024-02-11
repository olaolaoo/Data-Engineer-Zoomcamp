-- Create an external table using the Green Taxi Trip Records Data for 2022.
CREATE OR REPLACE EXTERNAL TABLE `coherent-ascent-379901.ny_taxi.external_green_tripdata`
OPTIONS(
    format = 'parquet',
    uris = ['gs://dtc-zoomcamp/green_tripdata_2022-*.parquet']
);

-- Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).

-- Question 1: What is count of records for the 2022 Green Taxi Data??

-- Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?


-- Question 3: How many records have a fare_amount of 0?


-- Question 4: What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

-- Question 5: Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
-- Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

