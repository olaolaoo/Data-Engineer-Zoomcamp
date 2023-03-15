>Previous:
>
> [01_Introduction](01_Introduction.md)
>
> [02_data_ingestion.md](02_data_ingestion.md)

>Next: [04_Analytics Engineering](4_Analytics Engineering.md)



See [week_3\_data_warehouse](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_3_data_warehouse) on GitHub.

Basic SQL:

- [BigQuery](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_3_data_warehouse/big_query.sql)
- [External
  Table](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_3_data_warehouse/big_query_hw.sql)
- [BigQuery
  ML](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_3_data_warehouse/big_query_ml.sql)

## 1.Concepts

### 1.1.OLAP vs OLTP

**OLTP** systems are designed to handle large volumes of transactional data involving multiple users.

**OLAP** system is designed to process large amounts of data quickly, allowing users to analyze multiple data dimensions
in tandem. Teams can use this data for decision-making and problem-solving.

|                     | **OLTP**-Online Transaction Processing                       | **OLAP**-Online Analytical Processing                        |
| ------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Purpose             | Control and run essential business operations in real time大量短小的事务数据进行快速处理和存储 | Plan, solve problems, support decisions, discover hidden insights海量数据中进行分析和提取出有价值的信息 |
| Data updates        | Short, fast updates initiated by user                        | Data periodically refreshed with scheduled, long-running batch jobs |
| Database design     | Normalized databases for efficiency(标准化数据库以提高效率)  | Denormalized databases for analysis(用于分析的非规范化数据库) |
| Space requirements  | Generally small if historical data is archived               | Generally large due to aggregating large datasets            |
| Backup and recovery | Regular backups required to ensure business continuity and meet legal and governance requirements | Lost data can be reloaded from OLTP database as needed in lieu of regular backups |
| Productivity        | Increases productivity of end users                          | Increases productivity of business managers, data analysts, and executives |
| Data view           | Lists day-to-day business transactions                       | Multi-dimensional view of enterprise data                    |
| User examples       | Customer-facing personnel, clerks, online shoppers           | Knowledge workers such as data analysts, business analysts, and executives |

### 1.2.What is a data wahehouse-OLAP solution

- OLAP solution

- Used for reporting and data analysis

  ![w3s03](images/03_bq2.png)

See also [The Data Warehouse Toolkit 3rd Edition](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/)
by Ralph Kimball and Margy Ross.

## 2.BigQuery

### 2.1.Overwiew

BigQuery is a fully managed(完全托管) enterprise data warehouse that helps you manage and analyze your data with built-in features like machine learning, geospatial analysis, and business intelligence.

- Serverless data warehouse(无服务器数据仓库)
  - There are no servers to manage or database software to install
- Software as well as infrastructure including
  - **scalability** and **high-availability**
- Built-in features like
  - machine learning
  - geospatial analysis
  - business intelligence
- BigQuery maximizes flexibility by separating the compute engine that analyzes your data from your storage
- On demand pricing
  - 1 TB of data processed is \$5
- Flat rate pricing
  - Based on number of pre requested slots
  - 100 slots → \$2,000/month = 400 TB data processed on demand pricing

See [What is BigQuery?](https://cloud.google.com/bigquery/docs/introduction) for more information.

### 2.2.Query Settings

![w3s03](images/03_bq1.png)

Click on **MORE** button, select **Query Settings**. In the **Resource management** section, we should disable **Use cached results**.

**File `big_query.sql`**

``` sql
-- Query public available table
SELECT station_id, name FROM
  bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;

-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://nyc-tl-data/trip data/yellow_tripdata_2019-*.csv', 'gs://nyc-tl-data/trip data/yellow_tripdata_2020-*.csv']
);
```



**File `big_query.sql`**

``` sql
-- Check yello trip data
SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata limit 10;
```

### 2.3.BigQuery Partition

A partitioned table is divided into segments, called partitions, that make it easier to manage and query your data. By dividing a large table into smaller partitions, you can improve query performance and control costs by reducing the number of bytes read by a query. You partition tables by specifying a partition column which is used to segment the table.

- Time-unit column
- Ingestion time (`_PARTITIONTIME`)
- Integer range partitioning
- When using Time unit or ingestion time
  - Daily (Default)
  - Hourly
  - Monthly or yearly
- Number of partitions limit is 4000

See [Introduction to partitioned tables](https://cloud.google.com/bigquery/docs/partitioned-tables) for more information.

**Partition in BigQuery**

![w3s05](images/03_bq3.png)

**File `big_query.sql`**

``` sql
-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE taxi-rides-ny.nytaxi.yellow_tripdata_non_partitoned AS
SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE taxi-rides-ny.nytaxi.yellow_tripdata_partitoned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata;
```

**File `big_query.sql`**

``` sql
-- Impact of partition
-- Scanning 1.6GB of data
SELECT DISTINCT(VendorID)
FROM taxi-rides-ny.nytaxi.yellow_tripdata_non_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Scanning ~106 MB of DATA
SELECT DISTINCT(VendorID)
FROM taxi-rides-ny.nytaxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';
```

**File `big_query.sql`**

``` sql
-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `nytaxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitoned'
ORDER BY total_rows DESC;
```

### BigQuery Clustering

Clustered tables in BigQuery are tables that have a user-defined column sort order using clustered columns. Clustered
tables can improve query performance and reduce query costs.

In BigQuery, a clustered column is a user-defined table property that sorts storage blocks based on the values in the
clustered columns. The storage blocks are adaptively sized based on the size of the table. A clustered table maintains
the sort properties in the context of each operation that modifies it. Queries that filter or aggregate by the clustered
columns only scan the relevant blocks based on the clustered columns instead of the entire table or table partition. As
a result, BigQuery might not be able to accurately estimate the bytes to be processed by the query or the query costs,
but it attempts to reduce the total bytes at execution.

See [Introduction to clustered tables](https://cloud.google.com/bigquery/docs/clustered-tables).

> 1:58/7:43 (3.1.2)

- Columns you specify are used to colocate related data
- Order of the column is important
- The order of the specified columns determines the sort order of the data.
- Clustering improves
  - Filter queries
  - Aggregate queries
- Table with data size \< 1 GB, don’t show significant improvement with partitioning and clustering
- You can specify up to four clustering columns

> 3.01/7:43 (3.1.2)

Clustering columns must be top-level, non-repeated columns:

- `DATE`
- `BOOL`
- `GEOGRAPHY`
- `INT64`
- `NUMERIC`
- `BIGNUMERIC`
- `STRING`
- `TIMESTAMP`
- `DATETIME`

> 19:38/23:04 (3.1.1)

**Clustering in BigQuery**

![w3s06](/Users/papa/de-zoomcamp-2023/dtc/w3s06.png)

**File `big_query.sql`**

``` sql
-- Creating a partition and cluster table
CREATE OR REPLACE TABLE taxi-rides-ny.nytaxi.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata;

-- Query scans 1.1 GB
SELECT count(*) as trips
FROM taxi-rides-ny.nytaxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

-- Query scans 864.5 MB
SELECT count(*) as trips
FROM taxi-rides-ny.nytaxi.yellow_tripdata_partitoned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;
```

### Partitioning vs Clustering

> 3:18/7:43 (3.1.2)

| **Clustering**                                               | **Partitoning**                       |
| ------------------------------------------------------------ | ------------------------------------- |
| Cost benefit unknown.                                        | Cost known upfront.                   |
| You need more granularity than partitioning alone allows.    | You need partition-level management.  |
| Your queries commonly use filters or aggregation against multiple particular columns. | Filter or aggregate on single column. |
| The cardinality of the number of values in a column or group of columns is large. |                                       |

### Clustering over paritioning

Like clustering, partitioning uses user-defined partition columns to specify how data is partitioned and what data is
stored in each partition. Unlike clustering, partitioning provides granular query cost estimates before you run a query.

See [Introduction to clustered tables](https://cloud.google.com/bigquery/docs/clustered-tables).

> 5:03/7:43 (3.1.2)

- Partitioning results in a small amount of data per partition (approximately less than 1 GB).
- Partitioning results in a large number of partitions beyond the limits on partitioned tables.
- Partitioning results in your mutation operations modifying the majority of partitions in the table frequently (for
  example, every few minutes).

### Automatic reclustering

> 6:04/7:43 (3.1.2)

As data is added to a clustered table

- The newly inserted data can be written to blocks that contain key ranges that overlap with the key ranges in
  previously written blocks
- These overlapping keys weaken the sort property of the table

To maintain the performance characteristics of a clustered table

- BigQuery performs automatic re-clustering in the background to restore the sort property of the table
- For partitioned tables, clustering is maintained for data within the scope of each partition.

### BigQuery Best Practice

> 0:00/3:44 (3.2.1)

- Cost reduction
  - Avoid `SELECT *`
  - Price your queries before running them
  - Use clustered or partitioned tables
  - Use streaming inserts with caution
  - Materialize query results in stages

In BigQuery, materialized views are precomputed views that periodically cache the results of a query for increased
performance and efficiency. BigQuery leverages precomputed results from materialized views and whenever possible reads
only delta changes from the base tables to compute up-to-date results. See [Introduction to materialized
views](https://cloud.google.com/bigquery/docs/materialized-views-intro)

> 1:56/3:44 (3.2.1)

- Query performance
  - Filter on partitioned columns
  - Denormalizing data
  - Use nested or repeated columns
  - Use external data sources appropriately
  - Don’t use it, in case u want a high query performance
  - Reduce data before using a `JOIN`
  - Do not treat `WITH` clauses as prepared statements
  - Avoid oversharding tables

> 2:43/3:44 (3.2.1)

- Query performance
  - Avoid JavaScript user-defined functions
  - Use approximate aggregation functions (HyperLogLog++)
  - Order Last, for query operations to maximize performance
  - Optimize your join patterns
  - As a best practice, place the table with the largest number of rows first, followed by the table with the fewest
    rows, and then place the remaining tables by decreasing size.

### Internals of BigQuery

> 0:00/6:05 (3.2.2)

**A high-level architecture for BigQuery service**

![BigQuery Architecture](/Users/papa/de-zoomcamp-2023/dtc/bigquery-architecture-1.png)

See also:

- [BigQuery under the hood](https://cloud.google.com/blog/products/bigquery/bigquery-under-the-hood)
- [A Deep Dive Into Google BigQuery Architecture](https://panoply.io/data-warehouse-guide/bigquery-architecture/)
- [BigQuery explained: An overview of BigQuery’s
  architecture](https://cloud.google.com/blog/products/data-analytics/new-blog-series-bigquery-explained-overview)

> 2:49/6:05 (3.2.2)

**Record-oriented vs column-oriented**

![w3s07](/Users/papa/de-zoomcamp-2023/dtc/w3s07.png)

BigQuery stores table data in columnar format, meaning it stores each column separately. Column-oriented databases are
particularly efficient at scanning individual columns over an entire dataset.

Column-oriented databases are optimized for analytic workloads that aggregate data over a very large number of records.
Often, an analytic query only needs to read a few columns from a table. See [Overview of BigQuery
storage](https://cloud.google.com/bigquery/docs/storage_overview) and [Storage
layout](https://cloud.google.com/bigquery/docs/storage_overview#storage_layout).

> 3:58/6:05 (3.2.2)

**An example of Dremel serving tree**

![w3s08](dtc/w3s08.png)

See [A Deep Dive Into Google BigQuery Architecture](https://panoply.io/data-warehouse-guide/bigquery-architecture/).

### BigQuery References

> 5:53/6:05 (3.2.2)

- [BigQuery How-to-guides](https://cloud.google.com/bigquery/docs/how-to)
- [Dremel: Interactive Analysis of Web-Scale Datasets](https://research.google/pubs/pub36632/)
- [A Deep Dive Into Google BigQuery Architecture](https://panoply.io/data-warehouse-guide/bigquery-architecture/)
- [A Look at Dremel](http://www.goldsborough.me/distributed-systems/2019/05/18/21-09-00-a_look_at_dremel/)