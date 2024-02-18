import requests



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


if __name__ == '__main__':
    # Use the generator to iterate over pages
    for page_data in paginated_getter():
        # Process each page as needed
        print(page_data)

# zen=paginated_getter()
# print(zen)
# print(next(zen))
# print(next(zen))
# .....