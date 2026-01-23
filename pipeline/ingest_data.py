#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"

DTYPE = {
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

PARSE_DATES = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]


def run(
    year: int,
    month: int,
    chunksize: int,
    pg_pass: str,
    pg_host: str,
    pg_user: str,
    pg_db: str,
    target_table: str,
    pg_port: int,
):
    url = f"{PREFIX}yellow_tripdata_{year}-{month:02d}.csv.gz"
    click.echo(f"Reading: {url}")

    engine = create_engine(f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

    # iterator over remote CSV
    df_iter = pd.read_csv(
        url,
        dtype=DTYPE,
        parse_dates=PARSE_DATES,
        iterator=True,
        chunksize=chunksize,
    )

    first = True
    total = 0

    for df_chunk in tqdm(df_iter, desc="Ingesting"):
        if first:
            # create/replace table schema
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace",
                index=False,
            )
            first = False

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
            index=False,
        )
        total += len(df_chunk)

    click.echo(f"Done. Inserted {total} rows into '{target_table}'.")


@click.command()
@click.option("--year", default=2021, type=int, show_default=True)
@click.option("--month", default=1, type=click.IntRange(1, 12), show_default=True)
@click.option("--chunksize", default=100000, type=int, show_default=True)
@click.option("--pg-pass", "pg_pass", default="root", show_default=True)
@click.option("--pg-host", "pg_host", default="localhost", show_default=True)
@click.option("--pg-user", "pg_user", default="root", show_default=True)
@click.option("--pg-db", "pg_db", default="ny_taxi", show_default=True)
@click.option("--target-table", "target_table", default="yellow_taxi_data", show_default=True)
@click.option("--pg-port", "pg_port", default=5432, type=int, show_default=True)
def main(year, month, chunksize, pg_pass, pg_host, pg_user, pg_db, target_table, pg_port):
    """Ingest NYC Yellow Taxi CSV data into Postgres."""
    run(year, month, chunksize, pg_pass, pg_host, pg_user, pg_db, target_table, pg_port)


if __name__ == "__main__":
    main()
