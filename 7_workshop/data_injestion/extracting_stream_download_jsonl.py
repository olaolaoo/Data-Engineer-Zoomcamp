#导入requests模块，用于发起 HTTP 请求
import requests
#导入 json 模块，用于处理 JSON 格式的数据
import json

def stream_download_jsonl(url):
    response = requests.get(url, stream=True)#使用 equests模块向指定的URL发起GET请求，并设置 stream=True 以启用流式传输
    response.raise_for_status()  # Raise an HTTPError for bad responses
    #iter_lines()逐行读取
    for line in response.iter_lines():
        #检查行是否存在数据
        if line:
            yield json.loads(line)#将解析后的数据逐行生成，以实现迭代器功能

# Replace the URL with your actual URL
url = "https://storage.googleapis.com/dtc_zoomcamp_api/yellow_tripdata_2009-06.jsonl"

# time the download
import time
start = time.time()

# Use the generator to iterate over rows with minimal memory usage
row_counter = 0
for row in stream_download_jsonl(url):
    # Process each row as needed
    print(row)
    row_counter += 1
    if row_counter >= 5:
        break

# time the download
end = time.time()
print(end - start)


