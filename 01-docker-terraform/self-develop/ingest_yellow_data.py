#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from sqlalchemy import create_engine, inspect
import click
import pyarrow.parquet as pq


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
    
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    
    # Check if table exists
    inspector = inspect(engine)
    table_exists = inspector.has_table(target_table)
    
    # Read the parquet file
    df = pd.read_parquet(url)
    
    if table_exists:
        # Append with chunked insertion
        print(f"Appending to existing table '{target_table}'...")
        chunksize = 100000
        total_rows = len(df)
        
        batch_num = 0
        for start in range(0, total_rows, chunksize):
            batch_num += 1
            chunk = df.iloc[start:start + chunksize]
            print(f"Inserting batch {batch_num} ({len(chunk):,} rows)...")
            
            chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists='append',
                index=False
            )
    else:
        # Create new table
        print(f"Creating new table '{target_table}'...")
        print(f"Loaded {len(df):,} rows")
        
        df.to_sql(
            name=target_table,
            con=engine,
            if_exists='replace',
            index=False
        )
    
    print(f"Done!")

if __name__ == '__main__':
    run()