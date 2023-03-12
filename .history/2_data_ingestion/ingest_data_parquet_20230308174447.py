#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pyarrow.parquet as pq

from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(user,password,host,port,db,table_name,url):
    
    csv_name = 'output.parquet'

    # download the CSV file
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # generator object returned by to_pandas()
    # df_iter is an iterator object, it can be used with the next() function to retrieve the next DataFrame object from the generator.
    
    df= pq.read_table(csv_name).to_pandas()

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')




if __name__ == '__main__':
        user='root'
        password='root'
        host='localhost'
        port=5432
        db='ny_taxi'
        table_name='yellow_taxi_trips_two'
        url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"

        main(user,password,host,port,db,table_name,url)