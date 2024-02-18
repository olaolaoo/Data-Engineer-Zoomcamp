#导入requests模块，用于发起 HTTP 请求
import requests
#导入 json 模块，用于处理 JSON 格式的数据
import json
import dlt
import duckdb

BASE_API_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
# I call this a paginated getter
# as it's a function that gets data
# and also paginates until there is no more data
# by yielding pages, we "microbatch", which speeds up downstream processing
def paginated_getter():
    page_number = 1

    while True:
        # Set the query parameters
        params = {'page': page_number}

        # Make the GET request to the API
        response = requests.get(BASE_API_URL, params=params)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        page_json = response.json()
        print(f'got page number {page_number} with {len(page_json)} records')

        # if the page has no records, stop iterating
        if page_json:
            #生成器将当前页面的 JSON 数据 page_json 返回，下面的__name__ == '__main__'拿到，执行print，进行下一个迭代，继续调用 paginated_getter() 函数
            yield page_json
            page_number += 1
        else:
            # No more data, break the loop
            break

# Replace the URL with your actual URL
url = "https://storage.googleapis.com/dtc_zoomcamp_api/yellow_tripdata_2009-06.jsonl"
def stream_download_jsonl(url):
    response = requests.get(url, stream=True)#使用 equests模块向指定的URL发起GET请求，并设置 stream=True 以启用流式传输
    response.raise_for_status()  # Raise an HTTPError for bad responses
    #iter_lines()逐行读取
    for line in response.iter_lines():
        #检查行是否存在数据
        if line:
            yield json.loads(line)#将解析后的数据逐行生成，以实现迭代器功能



# define the connection to load to.
# We now use duckdb, but you can switch to Bigquery later
generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='generators')
# we can load any generator to a table at the pipeline destnation as follows:
info = generators_pipeline.run(paginated_getter(),
										table_name="http_download",
										write_disposition="replace")
# the outcome metadata is returned by the load and we can inspect it by printing it.
print(info)

# we can load another generator to the same or to a different table.
info = generators_pipeline.run(stream_download_jsonl(url),
										table_name="stream_download",
										write_disposition="replace")
print(info)


# show outcome
conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")
# let's see the tables
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print('Loaded tables: ')
display(conn.sql("show tables"))

# and the data
print("\n\n\n http_download table below:")
rides = conn.sql("SELECT * FROM http_download").df()
display(rides)

# print("\n\n\n stream_download table below:")
# passengers = conn.sql("SELECT * FROM stream_download").df()
# display(passengers)



