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
    parquet_name = 'output.parquet'

    print(f"Downloading parquet from {url}...")
    os.system(f"wget {url} -O {parquet_name}")
    print("CSV download complete.\n")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    print(f"Reading Parquet file into Pandas DataFrame...")
    df = pq.read_table(parquet_name).to_pandas()
    print("Parquet file read complete.\n")

    print(f"Creating or replacing table '{table_name}' in PostgreSQL...")
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    print("Table creation/replacement complete.\n")

    print(f"Appending data to table '{table_name}' in PostgreSQL...")
    df.to_sql(name=table_name, con=engine, if_exists='append')
    print("Data append complete.\n")

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
