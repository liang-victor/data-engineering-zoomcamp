#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from sqlalchemy import create_engine
import argparse
import requests


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    file_url = params.url

    # download the file
    r = requests.get(file_url)
    file_name = 'output.parquet'
    with open(file_name, 'wb') as f:
        f.write(r.content)
        

    df = pd.read_parquet(file_name)
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()

    def iter_df(df, chunk_size):
        dataframe_size = len(df)
        max_range = (dataframe_size // chunk_size + 1) * chunk_size
        for index in range(0, max_range + 1, chunk_size):
            if index != 0:
                yield (df[previous_index:index])
            previous_index = index

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    for i, df_chunk in enumerate(iter_df(df, 100000)):
        print(f"processing chunk {i}")
        df_chunk.to_sql(name=table_name, con=engine, if_exists="append")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Parquet Data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='Database name')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)