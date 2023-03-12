>Previous: [01_Introduction](01_Introduction.md)

>Next: [03_data_warehouse](03_data_warehouse.md)

# Data Ingestion

This lesson will cover the topics of _Data Lake_ and _pipelines orchestration with *prefect*

# 1.Data Lake

_[Video source](https://www.youtube.com/watch?v=W3Zm6rjOq70&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=16)_

## *1.1.What* is a Data Lake?

![data lake](images/00_dl.png)

A ***Data Lake*** is a _central repository_ that holds _big data_ from many sources.

The _data_ in a Data Lake could either be structured, unstructured or a mix of both.

The main goal behind a Data Lake is being able to ingest data as quickly as possible and making it available to the other team members.

A Data Lake should be:

* Secure
* Scalable
* Able to run on inexpensive hardware

## *1.2.Data* Lake vs Data Warehouse

![data lake and data warehouse](images/00_dw_dl.png)



A Data Lake (DL) is not to be confused with a Data Warehouse (DW). There are several differences:

* Data Processing:
  * DL: The data is **raw** and has undergone minimal processing. The data is generally unstructured.
  * DW: the data is **refined**; it has been cleaned, pre-processed and structured for specific use cases.
* Size:
  * DL: Data Lakes are **large** and contains vast amounts of data, in the order of petabytes. Data is transformed when in use only and can be stored indefinitely.
  * DW: Data Warehouses are **small** in comparison with DLs. Data is always preprocessed before ingestion and may be purged periodically.
* Nature:
  * DL: data is **undefined** and can be used for a wide variety of purposes.
  * DW: data is historic and **relational**, such as transaction systems, etc.
* Users:
  * DL: Data scientists, data analysts.
  * DW: Business analysts.
* Use cases:
  * DL: Stream processing实时处理数据, machine learning, real-time analytics...
  * DW: Batch processing离线批量处理大量数据, business intelligence, BI reporting.

**Why did Data Lakes come into existence ？**

because as companies started to realize the importance of data, they soon found out that they couldn't ingest data right away into their DWs but they didn't want to waste uncollected data when their devs hadn't yet finished developing the necessary relationships for a DW, so the Data Lake was born to collect any potentially useful data that could later be used in later steps from the very start of any new projects.

* Companies realized the value of data 公司意识到数据的价值 
* Store and access data quickly 快速存储和访问数据 
* Cannot always define structure of data 不能总是定义数据的结构 
* Usefulness of data being realized later in the project lifecycle在项目生命周期后期实现的数据的有用性 
* ncrease in data scientists 数据科学家中的 lncrease 
* ﻿﻿R&D on data products研发数据产品 
* Need for Cheap storage of Big data 需要廉价存储大数据

## *1.3. ELT vs ETL*

When ingesting data, DWs use the ***Export, Transform and Load*** (ETL) model whereas DLs use ***Export, Load and Transform*** (ELT).

The main difference between them is the order of steps. In DWs, ETL (Schema on Write) means the data is _transformed_ (preprocessed, etc) before arriving to its final destination, whereas in DLs, ELT (Schema on read) the data is directly stored without any transformations and any schemas are derived when reading the data from the DL.

## *1.4.Data Swamp - Data Lakes gone wrong*

Data Lakes are only useful if data can be easily processed from it. Techniques such as versioning and metadata are very helpful in helping manage a Data Lake. A Data Lake risks degenerating into a ***Data Swamp*** if no such measures are taken, which can lead to bad results:

* No versioning of the data
* Incompatible schemas for the same data
* No metadata associated
* Joins between different datasets are not possible

## *1.5.Data Lake Cloud Providers*

* Google Cloud Platform > [Cloud Storage](https://cloud.google.com/storage)
* Amazon Web Services > [Amazon S3](https://aws.amazon.com/s3/)
* Microsoft Azure > [Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/)

# 2.Introduction to Workflow Orchestration

工作流编排工具

_[Video source](https://www.youtube.com/watch?v=0yK7LXwYeD0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=17)_

In the previous lesson we saw the definition of [data pipeline](1_intro.md#data-pipelines) and we created a [pipeline script](../1_intro/ingest_data.py) that downloaded a CSV and processed it so that we could ingest it to Postgres.

The script we created is an example of how **NOT** to create a pipeline,

* because it contains 2 steps which could otherwise be separated (downloading and processing). The reason is that if our internet connection is slow or if we're simply testing the script, it will have to download the CSV file every single time that we run the script, which is less than ideal.

Ideally, each of these steps would be contained as separate entities, like for example 2 separate scripts. For our pipeline, that would look like this:

```
(web) → DOWNLOAD → (csv) → INGEST → (Postgres)
```

We have now separated our pipeline into a `DOWNLOAD` script and a `INGEST` script.

In this lesson we will create a more complex pipeline:

```
(web)
  ↓
DOWNLOAD
  ↓
(csv)
  ↓
PARQUETIZE
  ↓
(parquet) ------→ UPLOAD TO S3
  ↓
UPLOAD TO GCS
  ↓
(parquet in GCS)
  ↓
UPLOAD TO BIGQUERY
  ↓
(table in BQ)
```

_Parquet_ is a [columnar storage datafile format](https://parquet.apache.org/) which is more efficient than CSV.

This ***Data Workflow*** has more steps and even branches. This type of workflow is often called a ***Directed Acyclic Graph*** (DAG)有向无环图, because it lacks any loops and the data flow is well defined.（因为它没有任何循环，数据流程是明确定义的。）

The steps in capital letters are our ***jobs*** and the objects in between are the jobs' outputs, which behave as ***dependencies*** for other jobs. Each job may have its own set of ***parameters*** and there may also be global parameters which are the same for all of the jobs.

A ***Workflow Orchestration Tool*** allows us to define data workflows and parametrize them; it also provides additional tools such as history and logging.

The tool we will focus on in this course is Prefect , but there are many others such as Luigi,  **[Apache Airflow](https://airflow.apache.org/)**,, Argo, etc.

# 3.Introduction to Prefect Concepts

Perfect is the modern open source data flow automation platform that's going to allow you to add observability and orchestration by using python just to write code as workflows and it's going to let you build run and monitor this pipeline at scale.

## *Method_1：without prefect,Manual import*

**Step1:建立ingest_data_parquet.py文档**

建立导入数据`ingest_data_parquet.py`文档，见[这个链接](../2_data_ingestion/ingest_data_parquet.py)

**Step2:docker中连接postgres**

**==tips：==**

* 在icloud中建立的ny_taxi_postgres_data文件夹，出现了权限bug

> "/var/lib/postgresql/data" has invalid permissions.
>
> 解决方法，应该是文件夹权限问题，所以在主机上新建ny_taxi_postgres_data，问题解决

```mkdir ny_taxi_postgres_data```

```
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    --network=pg-network \
    --name pg-database \
    postgres:13
```

**Step3:python手动导入**

打开postgres容器后，进入·ingest_data_parquet.py·所在文件夹，并在终端中输入：

```
python ingest_data_parquet.py
```

**Step4:检查是否导入数据**

在终端中输入`pgcli -h localhost -p 5432 -u root -d ny_taxi`，打开数据库，输入`\dt`查看所有数据表

## *Method_2：with prefect,Prefect Workfolw import*

***Step1:建立ingest_data_parquet_flow.py文档***

建立导入数据`ingest_data_parquet_flow.py`文档，见[这个链接](../2_data_ingestion/ingest_data_parquet_flow.py)

***Step2:检查是否安装prefect***

`prefect --version`

如果没有安装，在终端安装，`pip install prefect`

***Step3:docker中连接postgres***

**==tips：==**

* 在icloud中建立的ny_taxi_postgres_data文件夹，出现了权限bug

> "/var/lib/postgresql/data" has invalid permissions.
>
> 解决方法，应该是文件夹权限问题，所以在主机上新建ny_taxi_postgres_data，问题解决

```mkdir ny_taxi_postgres_data```

```
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    --network=pg-network \
    --name pg-database \
    postgres:13
```

==可能出现的问题==：输入上面的脚本，终端显示：```docker: Error response from daemon: network pg-network not found.``

解决方法：终端输入docker network create pg-network，这个在上一章关于在Connecting pgAdmin and Postgres with Docker networking学习过，点击[link](01_Introduction.md)直达

***Step4:启动Prefect Orion orchestration engine***

* 在终端中输入 `prefect orion start`

* 新建一个终端窗口，输入 `prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api`
* 打开 http://127.0.0.1:4200/ Orion interface

***Step5:安装prefect-sqlalchemy（如果 http://127.0.0.1:4200/ Orion interface，block下没有sqlalchemy）***

* 在终端中输入`pip install prefect-sqlalchemy`

* 在终端输入`prefect block register -m prefect_sqlalchemy`，重新进入 http://127.0.0.1:4200/界面block模块中，看到sqlalchemy出现

* 找到sqlalchemy，点击+，Write or select these parameters :

  - **Block Name**: postgres-connector
  - **Driver**: SyncDriver
  - **The driver name to use**: postgresql+psycopg2
  - **The name of the database to use**: ny_taxi
  - **Username**: root
  - **Password**: root
  - **Host**: localhost
  - **Port**: 5432

  Then click on the **Create** button

  可以看到下面的脚本，需要放在文档中，已经放啦

  ![sqlalchemy](images/00_sqlalchemy.png)

***Step6:导入数据***

在终端中输入`python ingest_data_parquet_flow.py`(进入py所在路径)

进程：

```bash
(base) papa@papadeMacBook-Pro 2_data_ingestion % python ingest_data_parquet_flow.py 
17:36:53.144 | INFO    | prefect.engine - Created flow run 'rainbow-manul' for flow 'Ingest Data'
17:36:53.389 | INFO    | Flow run 'rainbow-manul' - Created subflow run 'paper-cuscus' for flow 'Subflow'
17:36:53.448 | INFO    | Flow run 'paper-cuscus' - Logging Subflow for: yellow_taxi_trips_three
17:36:53.512 | INFO    | Flow run 'paper-cuscus' - Finished in state Completed()
17:36:53.538 | INFO    | Flow run 'rainbow-manul' - Created task run 'extract_data-0' for task 'extract_data'
17:36:53.539 | INFO    | Flow run 'rainbow-manul' - Executing 'extract_data-0' immediately...
--2023-03-09 17:36:53--  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
Resolving d37ci6vzurychx.cloudfront.net (d37ci6vzurychx.cloudfront.net)... 65.8.165.107, 65.8.165.201, 65.8.165.5, ...
Connecting to d37ci6vzurychx.cloudfront.net (d37ci6vzurychx.cloudfront.net)|65.8.165.107|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 21686067 (21M) [application/x-www-form-urlencoded]
Saving to: 'output.csv'

output.csv              100%[===============================>]  20.68M  4.84MB/s    in 4.3s    

2023-03-09 17:36:59 (4.84 MB/s) - 'output.csv' saved [21686067/21686067]

17:37:04.265 | INFO    | Task run 'extract_data-0' - Finished in state Completed()
17:37:04.294 | INFO    | Flow run 'rainbow-manul' - Created task run 'transform_data-0' for task 'transform_data'
17:37:04.294 | INFO    | Flow run 'rainbow-manul' - Executing 'transform_data-0' immediately...
17:37:04.366 | INFO    | Task run 'transform_data-0' - pre: missing passenger count: 26726
17:37:04.530 | INFO    | Task run 'transform_data-0' - post: missing passenger count: 0
17:37:04.564 | INFO    | Task run 'transform_data-0' - Finished in state Completed()
17:37:04.594 | INFO    | Flow run 'rainbow-manul' - Created task run 'load_data-0' for task 'load_data'
17:37:04.595 | INFO    | Flow run 'rainbow-manul' - Executing 'load_data-0' immediately...
17:37:04.855 | INFO    | Task run 'load_data-0' - Created a new engine.
17:37:05.362 | INFO    | Task run 'load_data-0' - Created a new connection.
17:40:48.765 | INFO    | Task run 'load_data-0' - Finished in state Completed()
17:40:48.829 | INFO    | Flow run 'rainbow-manul' - Finished in state Completed('All states completed.')
```

***Step7:检查是否导入数据***

在终端中输入`pgcli -h localhost -p 5432 -u root -d ny_taxi`，打开数据库，输入`\dt`查看所有数据表

```bash
(base) papa@papadeMacBook-Pro 2_data_ingestion % pgcli -h localhost -p 5432 -u root -d ny_taxi
Server: PostgreSQL 13.10 (Debian 13.10-1.pgdg110+1)
Version: 3.5.0
Home: http://pgcli.com
root@localhost:ny_taxi> \dt
+--------+-------------------------+-------+-------+
| Schema | Name                    | Type  | Owner |
|--------+-------------------------+-------+-------|
| public | yellow_taxi_trips       | table | root  |
| public | yellow_taxi_trips_three | table | root  |
| public | yellow_taxi_trips_two   | table | root  |
+--------+-------------------------+-------+-------+
SELECT 3
Time: 0.039s
root@localhost:ny_taxi> select count(1) from yellow_taxi_trips_three
+---------+
| count   |
|---------|
| 1343043 |
+---------+
SELECT 1
Time: 1.192s (1 second), executed in: 1.186s (1 second)
```

***Step8:关闭***

Ctrl+D to quit pgcli.

Ctrl+C to quit Orion.

# 4.ETL with GCP & Prefect

_[videocourse](https://www.youtube.com/watch?v=W-rMz_2GwqQ&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=20)_

## ***Step1:Start Prefect Orion***

终端输入`prefect orion start`

Check out the dashboard at [http://127.0.0.1:4200](http://127.0.0.1:4200/)

## ***Step2:Without GCP Buket***

***建立并运行etl_web_to_gcs.py文档***

建立导入数据`etl_web_to_gcs.py`文档，见[这个链接](../2_data_ingestion/etl_web_to_gcs.py)

终端输入`python etl_web_to_gcs.py`

过程:

```bash
(base) ola@192 2_data_ingestion % python etl_web_to_gcs.py
11:52:56.814 | INFO    | prefect.engine - Created flow run 'myrtle-oryx' for flow 'etl-web-to-gcs'
11:52:57.234 | INFO    | Flow run 'myrtle-oryx' - Created task run 'fetch-0' for task 'fetch'
11:52:57.236 | INFO    | Flow run 'myrtle-oryx' - Executing 'fetch-0' immediately...
--2023-03-12 11:52:57--  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
Resolving d37ci6vzurychx.cloudfront.net (d37ci6vzurychx.cloudfront.net)... 143.204.101.58, 143.204.101.63, 143.204.101.175, ...
Connecting to d37ci6vzurychx.cloudfront.net (d37ci6vzurychx.cloudfront.net)|143.204.101.58|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 21686067 (21M) [application/x-www-form-urlencoded]
Saving to: 'output_flow_etl.parquet'

output_flow_etl.parquet                            100%[================================================================================================================>]  20.68M  8.56MB/s    in 2.4s    

2023-03-12 11:53:01 (8.56 MB/s) - 'output_flow_etl.parquet' saved [21686067/21686067]

11:53:02.086 | INFO    | Task run 'fetch-0' - Finished in state Completed()
11:53:02.138 | INFO    | Flow run 'myrtle-oryx' - Created task run 'clean-0' for task 'clean'
11:53:02.141 | INFO    | Flow run 'myrtle-oryx' - Executing 'clean-0' immediately...
11:53:02.496 | INFO    | Task run 'clean-0' -    VendorID tpep_pickup_datetime tpep_dropoff_datetime  passenger_count  trip_distance  RatecodeID  ... tip_amount  tolls_amount  improvement_surcharge  total_amount  congestion_surcharge  airport_fee
0         1  2021-01-01 00:30:10   2021-01-01 00:36:12              1.0            2.1         1.0  ...        0.0           0.0                    0.3          11.8                   2.5          NaN
1         1  2021-01-01 00:51:20   2021-01-01 00:52:19              1.0            0.2         1.0  ...        0.0           0.0                    0.3           4.3                   0.0          NaN

[2 rows x 19 columns]
11:53:02.500 | INFO    | Task run 'clean-0' - columns: VendorID                          int64
tpep_pickup_datetime     datetime64[ns]
tpep_dropoff_datetime    datetime64[ns]
passenger_count                 float64
trip_distance                   float64
RatecodeID                      float64
store_and_fwd_flag               object
PULocationID                      int64
DOLocationID                      int64
payment_type                      int64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
improvement_surcharge           float64
total_amount                    float64
congestion_surcharge            float64
airport_fee                     float64
dtype: object
11:53:02.503 | INFO    | Task run 'clean-0' - rows: 1369769
11:53:02.567 | INFO    | Task run 'clean-0' - Finished in state Completed()
11:53:02.630 | INFO    | Flow run 'myrtle-oryx' - Created task run 'write_local-0' for task 'write_local'
11:53:02.633 | INFO    | Flow run 'myrtle-oryx' - Executing 'write_local-0' immediately...
11:53:07.652 | INFO    | Task run 'write_local-0' - Finished in state Completed()
11:53:07.696 | INFO    | Flow run 'myrtle-oryx' - Finished in state Completed('All states completed.')
```

## *Step2:With GCP Bucket*

* **建立一个bucket（在Cloud Storage下面），**在GCP的IM中建立一个service account，建立一个项目，也可以在老的服务账号里，如何建立一个Create service account：
  * On **Google Cloud Console**, select **IAM & Admin**, and **Service Accounts**. Then click on **+ CREATE SERVICE ACCOUNT** with these informations:
    - Service account details: zoom-de-service-account
    - Click on **CREATE AND CONTINUE** button.
    - Give the roles **BigQuery Admin** and **Storage Admin**.
    - Click on **CONTINUE** button.
    - Click on **DONE** button.
* **GCS Bucket Block**
  * 在**prefect**中建立**普通**GCS Bucket的**Block**
    * Inside Orion, select **Blocks** at the left menu, choose the block **GCS Bucket** and click **Add +** button. Complete the form with:
    * Block Name: zoom-gcs
    * Name of the bucket: dtc_data_lake_firstone==上面一步建立的bucket名字一样==
    * click create button
  * 在**prefect**中建立**加密GCS Bucket的**Block
    * Under **Gcp Credentials**, click on **Add +** button to create a **GCP Credentials** with these informations:
    * block Name: zoom-gcp-creds
    * 将service account中的key json内容贴进去

* **Modify our python program**

We then obtain a fragment of code to insert into our python code. Which allows us to add the `write_gcs` method to `etl_web_to_gcs.py`增加函数：

```python
@task
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcp")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return
```

* **Run the python program again**

`python etl_web_to_gcs.py`

* **back to GCP, check out the added data in your bucket**

Before leaving, I deleted my bucket.**？？？why did this man delete his bucket？？？**

`Ctrl+C` to stop Prefect Orion.

# 5.From Google Cloud Storage to Big Query

_[videocourse](https://www.youtube.com/watch?v=Cx5jt-V5sgE&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=21)_

Now let’s create another python program to load our data into the Google Cloud Storage (GCS) to Big Query.

## *Method_1：Manual setup In GCP*

![](images/00_bq1.png)

> choose big query from the left hamburger menu

![00_bq2](images/00_bq2.png)

>  choose Google Cloud Storage

![00_bq3](images/00_bq3.png)

> You need to decide the final part of Destination, except for the Project that should be choosen from existed ones

![00_bq4](images/00_bq4.png)

> check out the data using query, after the former steps

## *Method_2：Prefect Workfolw setup using python*

### ***Step1:Start Prefect Orion***

* type this command at the prompt：`prefect orion start`

* Check out the dashboard at [http://127.0.0.1:4200](http://127.0.0.1:4200/)

### *Step2:Modify and Run  python program*

* establishe`etl_gcs_to_bq.py`文档，见[这个链接](../2_data_ingestion/etl_gcs_to_bq.py)

  * in this document, there is some script like:

    ```python
    def write_bq(df: pd.DataFrame) -> None:
        """Write DataFrame to BiqQuery"""
    
        gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    
        df.to_gbq(
            destination_table="dezoomcamp.rides",
            project_id="fast-mariner-379909",
            credentials=gcp_credentials_block.get_credentials_from_service_account(),
            chunksize=500_000,
            if_exists="append",
        )
    ```

    `gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")`， ==you need to estabish a credential GCS Block in the Prefect, before run etl_gcs_to_bq.py.== How to do this  you can click here to find out[ 在**prefect**中建立**加密GCS Bucket的**Block](##Step2:With GCP Bucket)

* type this command at the prompt：`python etl_gcs_to_bq.py`

### *Step3:check out the outcome in GCP just like method_1*

Check out the big query , you should see the table you added in step2.

# 6.Parametrizing Flow & Deployments with ETL into GCS flow

We will see in this section:

- Parametrizing the script from your flow (rather then hard coded)
- Parameter validation with Pydantic
- Creating a deployment locally
- Running the flow
- Setting up Prefect Agent
- Notifications

## *Parametrizing the script from your flow*

## *Parameter validation with Pydantic*

## *Creating a deployment locally*

## *Running the flow*

## *Setting up Prefect Agent*

## *Notifications*

# 7.Schedules and Docker Storage with Infrastructure

We will see in this section:

- Scheduling a deployment
- Flow code storage
- Running tasks in Docker

## *Scheduling a deployment*

## *Flow code storage*

## *Running tasks in Docker*

# 8.Prefect Cloud and Additional Resources

We will see:

- Using Prefect Cloud instead of local Prefect
- Workspaces
- Running flows on GCP
