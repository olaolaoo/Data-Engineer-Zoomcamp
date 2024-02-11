-- Query public available table查询公共表
SELECT station_id, name FROM
  bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;

-- Creating external table referring to gcs path通过在gcs中的外部表新建表,格式project_name.dataset_name.tale_name
CREATE OR REPLACE EXTERNAL TABLE `coherent-ascent-379901.ny_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc-zoomcamp/yellow_tripdata_2019-*.csv','gs://dtc-zoomcamp/yellow_tripdata_2020-*.csv']
);

--check external_yellow_tripdata
SELECT * FROM coherent-ascent-379901.ny_taxi.external_yellow_tripdata limit 10;


-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE coherent-ascent-379901.ny_taxi.yellow_tripdata_non_partitoned AS
SELECT * FROM coherent-ascent-379901.ny_taxi.external_yellow_tripdata;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE coherent-ascent-379901.ny_taxi.yellow_tripdata_partitoned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM coherent-ascent-379901.ny_taxi.external_yellow_tripdata;

-- Impact of partition
-- Scanning 582.21 MB of data
SELECT DISTINCT(VendorID)
FROM coherent-ascent-379901.ny_taxi.yellow_tripdata_non_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-01' AND '2019-03-31';

-- Scanning 343.61 MB of DATA
SELECT DISTINCT(VendorID)
FROM coherent-ascent-379901.ny_taxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-01' AND '2019-03-31';

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `ny_taxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitoned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE coherent-ascent-379901.ny_taxi.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM coherent-ascent-379901.ny_taxi.external_yellow_tripdata;

-- Query scans 343.61 MB
SELECT count(*) as trips
FROM coherent-ascent-379901.ny_taxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
  AND VendorID=1;

-- Query scans 275.69 MB
SELECT count(*) as trips
FROM coherent-ascent-379901.ny_taxi.yellow_tripdata_partitoned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
  AND VendorID=1;


