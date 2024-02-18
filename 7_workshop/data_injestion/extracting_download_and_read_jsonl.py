import requests
import json

url = "https://storage.googleapis.com/dtc_zoomcamp_api/yellow_tripdata_2009-06.jsonl"

def download_and_read_jsonl(url):
    response = requests.get(url)
    response.raise_for_status()  # Raise an HTTPError for bad responses
    data = response.text.splitlines()# split json
    parsed_data = [json.loads(line) for line in data]
    return parsed_data


downloaded_data = download_and_read_jsonl(url)

if downloaded_data:
    # Process or print the downloaded data as needed
    print(downloaded_data[:5])  # Print the first 5 entries as an example