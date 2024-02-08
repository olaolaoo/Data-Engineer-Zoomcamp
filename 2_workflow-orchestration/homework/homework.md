## Week 2 Homework



## Question 1. Data Loading

> **Answer:266 855 rows x 20 columns**

data loader script  is down below, Once the dataset is loaded, what's the shape of the data is 266,855 rows x 20 columns

```python
import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url1 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-10.csv.gz'
    url2 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-11.csv.gz'
    url3 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-12.csv.gz'
    #response = requests.get(url)
    urls = [url1, url2, url3]  # List of URLs

    #return pd.read_csv(io.StringIO(response.text), sep=',')
    taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'RatecodeID':pd.Int64Dtype(),
                    'store_and_fwd_flag':str,
                    'PULocationID':pd.Int64Dtype(),
                    'DOLocationID':pd.Int64Dtype(),
                    'payment_type': pd.Int64Dtype(),
                    'fare_amount': float,
                    'extra':float,
                    'mta_tax':float,
                    'tip_amount':float,
                    'tolls_amount':float,
                    'improvement_surcharge':float,
                    'total_amount':float,
                    'congestion_surcharge':float
                }
    # native date parsing 
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    dfs = []  # List to store individual DataFrames
    # Loop over the URLs to read the data and append to the list of DataFrames
    for url in urls:
        df = pd.read_csv(url, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
        dfs.append(df)
    # Concatenate the list of DataFrames into a single DataFrame
    final_df = pd.concat(dfs, ignore_index=True)    
    # Display the resulting DataFrame
    #print(final_df)
    final_df.head()
    return final_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

```




## Question 2. Data Transformation

> **Answer:139370 rows**

Upon filtering the dataset where the passenger count is greater than 0 *and* the trip distance is greater than zero, how many rows are left?

data transformer script  is down below, after cleaning the data, 139370 rows are left:

```python
import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    #Remove rows where the passenger count is equal to 0 and the trip distance is equal to zero.
    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    #Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    data['lpep_pickup_datetime'] = pd.to_datetime(data['lpep_pickup_datetime'])
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    #Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id.
    data.columns = (data.columns.str.lower().str.replace('id','_id'))

    return data
    


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    data=output
    assert data['vendor_id'].isin(data['vendor_id']).all(), "Vendor ID not one of existing values"
    assert (data['passenger_count'] > 0).all(), "Passenger count is not greater than 0"
    assert (data['trip_distance'] > 0).all(), "Trip distance is not greater than 0"

```

## Question 3. Data Transformation

> **Answer:`data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date`**

## Question 4. Data Transformation

> **Answer:1 or 2**

What are the existing values of `VendorID` in the dataset?

>Command:
```python
unique_values = data['vendor_id'].unique()
print(f"vendor_id is {unique_values}")
```
## Question 5.Data Transformation

> **Answer:3**

How many columns need to be renamed to snake case?

I just counted.

## Question 6. Data Exporting

> **Answer:96**(95 subfoder+1 foder)

Once exported, how many partitions (folders) are present in Google Cloud?

>Command:
```python
unique_count = data['lpep_pickup_date'].nunique()
    print("subfoder is :", unique_count)
```
>**Answer :**
```
subfoder is : 95
```
## Submitting the solutions 

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw2

Deadline: Feb. 8, 2024, 11 p.m.
