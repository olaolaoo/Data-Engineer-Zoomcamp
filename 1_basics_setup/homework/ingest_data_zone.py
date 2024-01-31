#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pyarrow.parquet as pq
from time import time
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    print("Parameters provided:")
    print(f"User: {params.user}")
    print(f"Password: {params.password}")
    print(f"Host: {params.host}")
    print(f"Port: {params.port}")
    print(f"Database: {params.db}")
    print(f"Table Name: {params.table_name}")
    print(f"URL: {params.url}\n")

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'zone.csv'

    print(f"Downloading  from {url}...")
    os.system(f"wget {url} -O {csv_name}")
    print("CSV download complete.\n")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    print(f"Reading  file into Pandas DataFrame...")
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    
    

    print(f"Creating or replacing table '{table_name}' in PostgreSQL...")
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    print("Table creation/replacement complete.\n")

    print(f"Appending data to table '{table_name}' in PostgreSQL...")
    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        try:
            t_start = time()

            df = next(df_iter)

            
            
            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))
        except StopIteration:
            print('completed')
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the parquet file')
    args = parser.parse_args()

    print("\nStarting data ingestion process...\n")
    main(args)
