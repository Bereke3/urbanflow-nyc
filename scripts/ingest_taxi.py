
import argparse
import re
from pathlib import Path

import duckdb

# Project paths
ROOT = Path(__file__).resolve().parents[1]
DATABASE = ROOT / "data/warehouse/nyc_taxi.duckdb"


def ingest(month):
    filepath = ROOT / f"data/raw/yellow_tripdata_{month}.parquet"

    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    DATABASE.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DATABASE))

    try:
        # Create the raw schema and table
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        con.execute("""
            CREATE TABLE IF NOT EXISTS raw.yellow_trips (
                source_month VARCHAR,
                vendor_id BIGINT,
                pickup_datetime TIMESTAMP,
                dropoff_datetime TIMESTAMP,
                passenger_count DOUBLE,
                trip_distance DOUBLE,
                pu_location_id BIGINT,
                do_location_id BIGINT,
                payment_type BIGINT,
                fare_amount DOUBLE,
                total_amount DOUBLE
            )
        """)

        # Count source rows before loading
        source_count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(filepath)]
        ).fetchone()[0]

        if source_count == 0:
            raise ValueError("Source file is empty")

        # Atomic partition replacement
        con.execute("BEGIN TRANSACTION")

        try:
            con.execute(
                "DELETE FROM raw.yellow_trips WHERE source_month = ?",
                [month]
            )

            con.execute("""
                INSERT INTO raw.yellow_trips
                SELECT
                    ? AS source_month,
                    VendorID,
                    tpep_pickup_datetime,
                    tpep_dropoff_datetime,
                    passenger_count,
                    trip_distance,
                    PULocationID,
                    DOLocationID,
                    payment_type,
                    fare_amount,
                    total_amount
                FROM read_parquet(?)
            """, [month, str(filepath)])

            loaded_count = con.execute(
                "SELECT COUNT(*) FROM raw.yellow_trips WHERE source_month = ?",
                [month]
            ).fetchone()[0]

            if loaded_count != source_count:
                raise ValueError("Row count mismatch")

            con.execute("COMMIT")

        except Exception:
            con.execute("ROLLBACK")
            raise

        print(f"Month: {month}")
        print(f"Source rows: {source_count}")
        print(f"Loaded rows: {loaded_count}")
        print("Status: SUCCESS")

    finally:
        con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True)
    args = parser.parse_args()

    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", args.month):
        parser.error("Month must be YYYY-MM")

    ingest(args.month)
