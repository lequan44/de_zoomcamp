#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from sqlalchemy import create_engine, inspect
import click
import requests
import os


@click.command()
@click.option('--pg_user', default='root', help='PostgreSQL user')
@click.option('--pg_pass', default='root', help='PostgreSQL password')
@click.option('--pg_host', default='localhost', help='PostgreSQL host')
@click.option('--pg_port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg_db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2025, type=int, help='Year of the data')
@click.option('--month', default=11, type=int, help='Month of the data')
@click.option('--target_table', default='yellow_taxi_data', help='Target table name')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table):
    """Ingest NYC taxi data into PostgreSQL database."""
    prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
    url = prefix + f'yellow_tripdata_{year}-{month:02d}.parquet'
    local_file = f'yellow_tripdata_{year}-{month:02d}.parquet'

    print(f"Downloading {url}...")
    response = requests.get(url)
    with open(local_file, "wb") as f:
        f.write(response.content)
    
    print("Reading parquet file...")
    df = pd.read_parquet(local_file)
    
    # Remove quotes from column names
    df.columns = df.columns.str.replace('"', '')
    print(f"Read {len(df):,} rows")
    
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    
    # Check if table exists
    inspector = inspect(engine)
    table_exists = inspector.has_table(target_table)
    
    mode = 'append' if table_exists else 'replace'
    if table_exists:
        print(f"Appending to existing table '{target_table}'...")
    else:
        print(f"Creating new table '{target_table}'...")

    print(f"Inserting data...")
    df.to_sql(name=target_table, con=engine, if_exists=mode, index=False)
    
    print("Done!")
    
    # Remove file
    if os.path.exists(local_file):
        os.remove(local_file)

if __name__ == '__main__':
    run()