#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
import logging
from tqdm import tqdm

pipeline_logger = logging.getLogger(__name__)
logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
)

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]


@click.command()
@click.option("--pg-user", default="root", help="PostgreSQL user")
@click.option("--pg-pass", default="root", help="PostgreSQL password")
@click.option("--pg-host", default="localhost", help="PostgreSQL host")
@click.option("--pg-port", default=5432, type=int, help="PostgreSQL port")
@click.option("--pg-db", default="ny_taxi", help="PostgreSQL database name")
@click.option("--year", default=2021, type=int, help="Year of the data")
@click.option("--month", default=1, type=int, help="Month of the data")
@click.option("--target-table", default="yellow_taxi_trips", help="Target table name")
@click.option(
    "--chunksize", default=100000, type=int, help="Chunk size for reading file"
)
def run(
    pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize
):
    
    """Ingest NYC taxi data into PostgreSQL database."""
    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    url = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"

    connection_string = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"

    pipeline_logger.info(f"Connecting to Postgre DB with connection string: {connection_string}")
    engine = create_engine(
        connection_string
    )

    pipeline_logger.info(f"Reading CSV at {url}")
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )

    first = True

    pipeline_logger.info("Writing to DB in chunks")

    for i, df_chunk in enumerate(tqdm(df_iter)):

        pipeline_logger.info(f"Loading chunk no. {i + 1}")
        if first:
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists="replace")
            first = False

        df_chunk.to_sql(name=target_table, con=engine, if_exists="append")


if __name__ == "__main__":
    run()
