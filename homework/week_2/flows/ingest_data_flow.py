#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from sqlalchemy import create_engine
import argparse
import requests
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector
from datetime import timedelta

@task(log_prints=True, tags=["extract"], cache_key_fn= task_input_hash, cache_expiration=timedelta(days=1), retries=3)
def extract_data(url: str):
    r = requests.get(url)
    file_name = 'output.parquet'
    with open(file_name, 'wb') as f:
        f.write(r.content)
    df = pd.read_parquet(file_name)
    return df

@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df

@task(log_prints=True, retries=3)
def load_data(table_name, df):
    
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')

@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging Subflow for: {table_name}")

@flow(name = "Ingest Data")
def main_flow(table_name: str, url: str):
    log_subflow(table_name)
    raw_data = extract_data(url)
    data = transform_data(raw_data)
    load_data(table_name, data)

if __name__ == '__main__':
    main_flow(table_name="yellow_taxi_trips", url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-01.parquet")