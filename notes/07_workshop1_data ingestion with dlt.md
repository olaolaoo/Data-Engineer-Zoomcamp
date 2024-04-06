# Workshop1-Data ingestion with dlt(data load tool)

You can get more details about [workshop of Data ingestion with dlt](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2024/workshops/dlt.md)

## 1.1.What's data ingestion

Data ingestion is the process of extracting data from a producer, transporting it to a convenient environment, and preparing it for usage by normalising it, sometimes cleaning, and adding metadata.

【数据摄取是从生产者提取数据、将其传输到便捷的环境并准备好供使用的过程，通常通过规范化、有时进行清洗和添加元数据来实现。也就是说ingestion包含了extracting data，像ETL或ELT是ingestion】

Here’s what you need to learn to build pipelines

- Extracting data
- Normalising, cleaning, adding metadata such as schema and types
- Incremental loading, which is vital for fast, cost effective data refreshes.

## 1.2.What else does a data engineer do? 

- It might seem simplistic, but in fact a data engineer’s main goal is to ensure data flows from source systems to analytical destinations.
- So besides building pipelines, running pipelines and fixing pipelines, a data engineer may also focus on optimising data storage, ensuring data quality and integrity, implementing effective data governance practices, and continuously refining data architecture to meet the evolving needs of the organisation.
- Ultimately, a data engineer's role extends beyond the mechanical aspects of pipeline development, encompassing the strategic management and enhancement of the entire data lifecycle.
- This workshop focuses on building robust, scalable, self maintaining pipelines, with built in governance - in other words, best practices applied.

## 1.3.What is dlt?

- dlt is an open-source library that you can add to your Python scripts to load data from various and often messy data sources into well-structured, live datasets.【dlt是一个开源的库，你可以使用它把不同的数据源加载成结构良好的数据集】
- You can install it using pip and there's no need to start any backends or containers. You can simply import dlt in your Python script and write a simple pipeline to load data from sources like APIs, databases, files, etc. into a destination of your choice.

Here are a few reasons why you should use dlt:

- Automated maintenance: With schema inference and evolution and alerts, and with short declarative code, maintenance becomes simple.
- Run it where Python runs: You can use dlt on Airflow, serverless functions, notebooks. It doesn't require external APIs, backends or containers, and scales on both micro and large infrastructures.
- User-friendly, declarative interface: dlt provides a user-friendly interface that removes knowledge obstacles for beginners while empowering senior professionals.

Benefits: As a data engineer, dlt offers several benefits:

- Efficient Data Extraction and Loading: dlt simplifies the process of extracting and loading data. It allows you to decorate your data-producing functions with loading or incremental extraction metadata, enabling dlt to extract and load data according to your custom logic. This is particularly useful when dealing with large datasets, as dlt supports scalability through iterators, chunking, and parallelization. Read more
- Automated Schema Management: dlt automatically infers a schema from data and loads the data to the destination. It can easily adapt and structure data as it evolves, reducing the time spent on maintenance and development. This ensures data consistency and quality. Read more
- Data Governance Support: dlt pipelines offer robust governance support through pipeline metadata utilization, schema enforcement and curation, and schema change alerts. This promotes data consistency, traceability, and control throughout the data processing lifecycle. Read more
- Flexibility and Scalability: dlt can be used on Airflow, serverless functions, notebooks, and scales on both micro and large infrastructures. It also offers several mechanisms and configuration options to scale up and fine-tune pipelines. Read more
- Post-Loading Transformations: dlt provides several options for transformations after loading the data, including using dbt, the dlt SQL client, or Pandas. This allows you to shape and manipulate the data before or after loading it, allowing you to meet specific requirements and ensure data quality and consistency. Read more

```python
pip install dlt[duckdb] # Install dlt with all the necessary DuckDB dependencies
```

## 1.4.How to ingest

### 1.4.1.Extracting data

In this section we will learn about extracting data from source systems, and what to care about when doing so.

#### **（1）The considerations of extracting data**

【注意事项】

* Most data is stored behind an API【大部分数据保存在API中】

  - Sometimes that’s a **RESTful api** for some business application, returning records of data.【api中保存了一条条的数据记录，不是一个文件】

  - Sometimes the API returns a secure file path to something like a json or parquet file in a bucket that enables you to grab the data in bulk【api中将数据保存成一个文件】

  - Sometimes the API is something else (mongo, sql, other databases or applications) and will generally return records as JSON - the most common interchange format.

* As an engineer, you will need to build pipelines that “just work”.【工程师要确保建立的pipeline的运行流畅】

  * So here’s what you need to consider on extraction, to prevent the pipelines from breaking, and to keep them running smoothly.【如何确保pipeline的流畅性，需要注意：】

    - Hardware limits: During this course we will cover how to navigate the challenges of managing memory.【硬盘限制】

    - Network limits: Sometimes networks can fail. We can’t fix what could go wrong but we can retry network jobs until they succeed. For example, dlt library offers a requests “replacement” that has built in retries. [Docs](https://dlthub.com/docs/reference/performance#using-the-built-in-requests-client). We won’t focus on this during the course but you can read the docs on your own.【网络限制】

    - Source api limits: Each source might have some limits such as how many requests you can do per second. We would call these “rate limits”. Read each source’s docs carefully to understand how to navigate these obstacles. You can find some examples of how to wait for rate limits in our verified sources repositories.
      - examples: [Zendesk](https://developer.zendesk.com/api-reference/introduction/rate-limits/), [Shopify](https://shopify.dev/docs/api/usage/rate-limits)

#### **（2）Extracting data without hitting hardware limits**

【通过streaming the data控制最大内存，避免触发硬盘限制】

**Control the max memory used by streaming the data**

Streaming here refers to processing the data event by event or chunk by chunk instead of doing bulk operations.

Let’s look at some classic examples of streaming where data is transferred chunk by chunk or event by event

- Between an audio broadcaster and an in-browser audio player
- Between a server and a local video player
- Between a smart home device or IoT device and your phone
- between google maps and your navigation app
- Between instagram live and your followers

What do data engineers do? We usually stream the data between buffers, such as

- from API to local file
- from webhooks to event queues
- from event queue (Kafka, SQS) to Bucket

**Streaming in python via generators**【使用python的generator】

Let’s focus on how we build most data pipelines:

- To process data in a stream in python, we use generators, which are functions that can return multiple times - by allowing multiple returns, the data can be released as it’s produced, as stream, instead of returning it all at once as a batch.

#### **（3）Examples**

**------------Example 1: Grabbing data from an api------------**

The api documentation is as follows:【api文档如下】

- There are a limited nr of records behind the api
- The data can be requested page by page, each page containing 1000 records
- If we request a page with no data, we will get a successful response with no data
- so this means that when we get an empty page, we know there is no more data and we can stop requesting pages - this is a common way to paginate but not the only one - each api may be different.
- details:
  - method: get
  - url: `https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api`
  - parameters: `page` integer. Represents the page number you are requesting. Defaults to 1.

So how do we design our requester?【如何设计】

- We need to request page by page until we get no more data. At this point, we do not know how much data is behind the api.
- It could be 1000 records or it could be 10GB of records. So let’s grab the data with a generator to avoid having to fit an undetermined amount of data into ram.

In this approach to grabbing data from apis, we have pros and cons:【优缺点】

- Pros: **Easy memory management** thanks to api returning events/pages【简单的内存管理】
- Cons: **Low throughput**, due to the data transfer being constrained via an API.【低吞吐量】
  - 吞吐量是指在一定时间内，系统、服务或者网络能够处理或传输的数据**量**或事务数量。用来衡量效率，在数据处理中，指数据处理的效率。

[extracting_paginated_getter.py](../7_workshop/data_injestion/extracting_paginated_getter.py) , in this example, we use yield to make a generator, 我理解的yield是会生成一个生成器，每次只展示当前的数据，如果想要展示下一个需要调用next()，可以将其改写成一个for循环，不停打印。 more detail about [yield](https://www.youtube.com/watch?v=HnggP09mKpM)

**------------Example 2:  The "bad" way to download a file------------**

Some apis respond with files instead of pages of data. The reason for this is simple: Throughput and cost. A restful api that returns data has to read the data from storage and process and return it to you by some logic - If this data is large, this costs time, money and creates a bottleneck.【因为1中的数据是以一条条的数据页形式存储在api中，所以如果数据量很大，则会提高时间和金钱成本】

A better way is to offer the data as files that someone can download from storage directly, without going through the restful api layer. This is common for apis that offer large volumes of data, such as ad impressions data.【一个更好的方法是直接从api中下载数据文档，但是如果数据文档很大，这也不是一个好的方法】

- Pros: **High throughput**
- Cons: **Memory** is used to hold all the data

In this example, we grab exactly the same data as we did in the API example above, but now we get it from the underlying file instead of going through the API.

[extracting_download_and_read_jsonl.py](../7_workshop/data_injestion/extracting_download_and_read_jsonl.py)

**------------Example 3: Extracting file data with a generator "the best practice way"------------**

Ok, downloading files is simple, but what if we want to do a stream download?

That’s possible too - in effect giving us the best of both worlds. In this case we prepared a jsonl file which is already split into lines making our code simple. But json (not jsonl) files could also be downloaded in this fashion, for example using the `json` library.

What are the pros and cons of this method of grabbing data?

Pros: **High throughput, easy memory management,** because we are downloading a file

Cons: **Difficult to do for columnar file formats**, as entire blocks need to be downloaded before they can be deserialised to rows. Sometimes, the code is complex too.【对于行式存储的表来说是优势，**对于列式文件格式来说比较困难**，因为在将其反序列化为行之前，整个数据块都需要被下载。有时代码也会很复杂。】[行式存储和列式存储的区别](https://blog.csdn.net/weixin_39400958/article/details/125147857)

Here’s what the code looks like - in a jsonl file each line is a json document, or a “row” of data, so we yield them as they get downloaded. This allows us to download one row and process it before getting the next row.

[extracting_stream_download_jsonl.py](../7_workshop/data_injestion/extracting_stream_download_jsonl.py)

**------------Loading the generator (any of the above)------------**

We have 3 ways to download the same data. Let's use the fast and reliable way to load some data and inspect it in [DuckDB](https://hightouch.com/blog/duckdb).【内存数据库，暂时存储，相当于开发环境中的数据库，最终会把数据load到生产环境数据仓库或数据湖，例如BigQuery或GCS】

In this example, we are using `dlt` library to do the loading, which will process data from the generators incrementally, following the same memory management paradigm.

[load_to_DuckDB.py](../7_workshop/data_injestion/load_to_DuckDB.py)

### 1.4.2.Normalising data

【规范化数据】

You often hear that data people spend most of their time “cleaning” data. What does this mean?【清洗数据】

Let’s look granularly into what people consider data cleaning.

Usually we have 2 parts:

- Normalising data without changing its meaning, 【规范，不改变含义】
- and filtering data for a use case, which changes its meaning.【过滤，改变含义】

#### （1）Part of what we often call data cleaning is just metadata work:

- Add types (string to number, string to timestamp, etc)
- Rename columns: Ensure column names follow a supported standard downstream - such as no strange characters in the names.
- Flatten nested dictionaries: Bring nested dictionary values into the top dictionary row
- Unnest lists or arrays into child tables: Arrays or lists cannot be flattened into their parent record, so if we want flat data we need to break them out into separate tables.
- We will look at a practical example next, as these concepts can be difficult to visualise from text.

#### （2）**Why prepare data? why not use json as is?**

【json毛病多，不好用，需要被清洗】

- We do not easily know what is inside a json document due to lack of schema【由于缺乏模式，我们很难知道 JSON 文档内部包含什么】
- Types are not enforced between rows of json - we could have one record where age is `25`and another where age is `twenty five` , and another where it’s `25.00`. Or in some systems, you might have a dictionary for a single record, but a list of dicts for multiple records. This could easily lead to applications downstream breaking.
- We cannot just use json data easily, for example we would need to convert strings to time if we want to do a daily aggregation.
- Reading json loads more data into memory, as the whole document is scanned - while in parquet or databases we can scan a single column of a document. This causes costs and slowness.
- Json is not fast to aggregate - columnar formats are.【JSON 聚合速度不快 - 列式格式则较快】
- Json is not fast to search.【JSON 搜索速度不快】
- Basically json is designed as a "lowest common denominator format" for "interchange" / data transfer and is unsuitable for direct analytical usage.

#### （3）example

dlt is a python library created for the purpose of assisting data engineers to build simpler, faster and more robust pipelines with minimal effort.

dlt automates much of the tedious work a data engineer would do, and does it in a way that is robust.

dlt can handle things like:

- Schema: Inferring and evolving schema, alerting changes, using schemas as data contracts.
- Typing data, flattening structures, renaming columns to fit database standards.【dbt会根据你的数据生成正确的数据格式】
- Processing a stream of events/rows without filling memory. This includes extraction from generators. In our example we will pass the “data” you can see above.
- Loading to a variety of dbs of file formats.

Read more about dlt [here](https://colab.research.google.com/corgiredirector?site=https%3A%2F%2Fdlthub.com%2Fdocs%2Fintro).

Now let’s use it to load our nested json to duckdb:

Here’s how you would do that on your local machine.

**First, install dlt**

```bash
# Make sure you are using Python 3.8-3.11 and have pip installed
# spin up a venv, 使用venv 模块创建一个名为 "env" 的虚拟环境。
python -m venv ./env
# 激活指定目录下的虚拟环境
source ./env/bin/activate
# pip install
pip install "dlt[duckdb]"
```

We want to load this data to a database. How do we want to clean the data?

- We want to flatten dictionaries into the base row

- We want to flatten lists into a separate table

- We want to convert time strings into time type

  ```python
  data = [
      {
          "vendor_name": "VTS",
  				"record_hash": "b00361a396177a9cb410ff61f20015ad",
          "time": {#嵌套的字典会生成新的字段,nested dictionaries could be flattened
              "pickup": "2009-06-14 23:23:00",#time__pickup
              "dropoff": "2009-06-14 23:48:00"
          },
          "Trip_Distance": 17.52,
          "coordinates": {
              "start": {
                  "lon": -73.787442,
                  "lat": 40.641525
              },
              "end": {
                  "lon": -73.980072,
                  "lat": 40.742963
              }
          },
          "Rate_Code": None,
          "store_and_forward": None,
          "Payment": {
              "type": "Credit",
              "amt": 20.5,
              "surcharge": 0,
              "mta_tax": None,
              "tip": 9,
              "tolls": 4.15,
  			"status": "booked"
          },
          "Passenger_Count": 2,
        	# 嵌套列表会生成新的表，nested lists need to be expressed as separate tables
          "passengers": [
              {"name": "John", "rating": 4.9},
              {"name": "Jack", "rating": 3.9}
          ],
          "Stops": [
              {"lon": -73.6, "lat": 40.6},
              {"lon": -73.5, "lat": 40.5}
          ]
      },
  ]
  ```

**Next, grab your data from above and run this snippet**

- here we define a pipeline, which is a connection to a destination

- and we run the pipeline, printing the outcome

  ```python
  # define the connection to load to. 
  # We now use duckdb, but you can switch to Bigquery later
  pipeline = dlt.pipeline(pipeline_name="taxi_data",
  						destination='duckdb', 
  						dataset_name='taxi_rides')
  
  # run the pipeline with default settings, and capture the outcome
  info = pipeline.run(data, 
                      table_name="users", 
                      write_disposition="replace")
  
  # show the outcome
  print(info)
  ```

  the whole script which you can check [normalize_load_to_DuckDB.py](../7_workshop/data_injestion/normalize_load_to_DuckDB.py)

  you can use the built in streamlit app by running the cli command with the pipeline name we chose above.

  ```bash
  dlt pipeline taxi_data show
  ```

  Or explore the data in the linked [colab notebook](https://colab.research.google.com/drive/1kLyD3AL-tYf_HqCXYnA3ZLwHGpzbLmoj#scrollTo=N9PrR_edOvSw&forceEdit=true&sandboxMode=true).

### 1.4.3.Incremental loading data

在规范化的基础上，更改payment_status，采用merge的方式增量数据

the whole script which you can check[incremental_load_to_DuckDB.py](../7_workshop/data_injestion/incremental_load_to_DuckDB.py)

## 1.5.What’s next?

- You could change the destination to parquet + local file system or storage bucket. See the[colab notebook](https://colab.research.google.com/drive/1kLyD3AL-tYf_HqCXYnA3ZLwHGpzbLmoj#scrollTo=N9PrR_edOvSw&forceEdit=true&sandboxMode=true) bonus section.
- You could change the destination to BigQuery. Destination & credential setup docs: https://dlthub.com/docs/dlt-ecosystem/destinations/, https://dlthub.com/docs/walkthroughs/add_credentials or See the [colab notebook](https://colab.research.google.com/drive/1kLyD3AL-tYf_HqCXYnA3ZLwHGpzbLmoj#scrollTo=N9PrR_edOvSw&forceEdit=true&sandboxMode=true) bonus section.
- You could use a decorator to convert the generator into a customised dlt resource: https://dlthub.com/docs/general-usage/resource
- You can deep dive into building more complex pipelines by following the guides:
  - https://dlthub.com/docs/walkthroughs
  - https://dlthub.com/docs/build-a-pipeline-tutorial

