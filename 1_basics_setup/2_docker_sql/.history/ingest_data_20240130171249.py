#!/usr/bin/env python
# coding: utf-8

#set up library
import os
import argparse
import pyarrow.parquet as pq

from time import time

import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_name = 'output.parquet'
    
    #download the file
    os.system(f"wget {url} -O {parquet_name}")
    # （：//用户名：密码@主机名：端口/数据库名称）
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df= pq.read_table(parquet_name).to_pandas()

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


if __name__ == '__main__':
    # Parse the command line arguments and calls the main program
    # We then create an ArgumentParser object, which is used to define our command-line arguments and options.
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    #We add  arguments using the add_argument method:
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    #we call the parse_args method to parse the command-line arguments and options provided by the user.
    args = parser.parse_args()

    main(args)



