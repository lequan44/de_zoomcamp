#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from sqlalchemy import create_engine, inspect
import click
import requests


dtype = {
    'LocationID': 'Int64',
    'Borough': 'string',
    'Zone': 'string',
    'service_zone': 'string'
}


@click.command()
@click.option('--pg_user', default='root', help='PostgreSQL user')
@click.option('--pg_pass', default='root', help='PostgreSQL password')
@click.option('--pg_host', default='localhost', help='PostgreSQL host')
@click.option('--pg_port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg_db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target_table', default='zones', help='Target table name')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table):
    """Ingest NYC taxi zone data into PostgreSQL database."""
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
    
    print(f"Reading CSV from {url}...")
    df = pd.read_csv(url, dtype=dtype)
    print(f"Read {len(df)} rows")

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    
    print(f"Ingesting into table '{target_table}'...")
    df.to_sql(name=target_table, con=engine, if_exists='replace', index=False)
    
    print("Done!")

if __name__ == '__main__':
    run()