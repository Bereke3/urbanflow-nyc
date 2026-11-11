
import os
import tempfile
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]

SOURCE = ROOT / "data/warehouse/nyc_taxi.duckdb"
OUTPUT = ROOT / "data/bi/nyc_taxi_bi.duckdb"

EXPECTED_ROWS = 313375
EXPECTED_TRIPS = 10701886

OUTPUT.parent.mkdir(parents=True, exist_ok=True)

with tempfile.TemporaryDirectory(dir=OUTPUT.parent) as temp_dir:
    snapshot = Path(temp_dir) / "snapshot.duckdb"

    con = duckdb.connect()

    try:
        # Read source database without modifying it
        con.execute(
            "ATTACH ? AS src (READ_ONLY)",
            [str(SOURCE)]
        )

        # Create a database compatible with DuckDB 1.5
        con.execute(
            "ATTACH ? AS bi (STORAGE_VERSION 'v1.5.0')",
            [str(snapshot)]
        )

        con.execute("CREATE SCHEMA bi.analytics")

        con.execute("""
            CREATE TABLE bi.analytics.fct_taxi_weather_hourly AS
            SELECT *
            FROM src.analytics.fct_taxi_weather_hourly
        """)

        rows, trips = con.execute("""
            SELECT COUNT(*), SUM(trip_count)
            FROM bi.analytics.fct_taxi_weather_hourly
        """).fetchone()

        
        # Compare exported data with the source
        expected_rows, expected_trips = con.execute("""
            SELECT
                COUNT(*),
                SUM(trip_count)
            FROM src.analytics.fct_taxi_weather_hourly
        """).fetchone()

        if expected_rows == 0 or expected_trips is None:
            raise ValueError("Source analytics table is empty")

        if (rows, trips) != (expected_rows, expected_trips):
            raise ValueError("BI snapshot validation failed")


        
        # Store snapshot metadata for monitoring
        con.execute("""
            CREATE TABLE bi.analytics.export_metadata AS
            SELECT
                CURRENT_TIMESTAMP AT TIME ZONE 'UTC'
                    AS exported_at_utc,
                COUNT(*) AS total_rows,
                SUM(trip_count) AS total_trips
            FROM bi.analytics.fct_taxi_weather_hourly
        """)


    finally:
        con.close()

    # Publish only after the copy has been validated
    os.replace(snapshot, OUTPUT)

print(f"Rows: {rows:,}")
print(f"Trips: {trips:,}")
print("BI snapshot: SUCCESS")
print(f"Saved to: {OUTPUT}")
