
import argparse
import subprocess
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATABASE = ROOT / "data/warehouse/nyc_taxi.duckdb"

PERIOD = "2025_Q1"
EXPECTED_HOURS = 2184


def run_script(filename):
    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / filename)],
        check=True
    )


def cached_weather_hours():
    con = duckdb.connect(str(DATABASE))

    try:
        result = con.execute("""
            SELECT COUNT(*)
            FROM raw.weather_hourly
            WHERE weather_hour_utc >=
                TIMESTAMPTZ '2025-01-01 00:00:00+00'
              AND weather_hour_utc <
                TIMESTAMPTZ '2025-04-02 00:00:00+00'
        """).fetchone()

        return result[0]

    except duckdb.CatalogException:
        return 0

    finally:
        con.close()


def save_status(status, error_message=None):
    con = duckdb.connect(str(DATABASE))

    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        con.execute("""
            CREATE TABLE IF NOT EXISTS raw.weather_refresh_status (
                period_id VARCHAR PRIMARY KEY,
                attempted_at_utc TIMESTAMPTZ,
                status VARCHAR,
                error_message VARCHAR
            )
        """)

        con.execute("BEGIN TRANSACTION")

        try:
            con.execute("""
                DELETE FROM raw.weather_refresh_status
                WHERE period_id = ?
            """, [PERIOD])

            con.execute("""
                INSERT INTO raw.weather_refresh_status
                VALUES (?, CURRENT_TIMESTAMP, ?, ?)
            """, [PERIOD, status, error_message])

            con.execute("COMMIT")

        except Exception:
            con.execute("ROLLBACK")
            raise

    finally:
        con.close()



def refresh_weather(simulate_failure=False):

    # Reuse complete historical weather data.
    # Do not skip explicit failure simulations.
    if not simulate_failure:
        hours = cached_weather_hours()

        if hours == EXPECTED_HOURS:
            save_status("CACHED_COMPLETE")

            print("Historical weather already cached")
            print(f"Cached weather hours: {hours}")
            print("Skipping Open-Meteo API request")
            return

    error = None


    if simulate_failure:
        error = "Simulated Open-Meteo API failure"

    else:
        try:
            run_script("fetch_weather.py")

        except subprocess.CalledProcessError as exc:
            error = f"Weather download failed: exit code {exc.returncode}"

    if error is not None:
        hours = cached_weather_hours()

        if hours == EXPECTED_HOURS:
            status = "API_FAILED_CACHED"
        else:
            status = "API_FAILED_INCOMPLETE"

        save_status(status, error)

        print(f"WARNING: {error}")
        print(f"Cached weather hours: {hours}")
        print(f"Weather status: {status}")
        print("Taxi pipeline can continue")
        return

    # Download succeeded: validate and ingest data.
    # Unexpected ingestion errors must fail the task.
    run_script("ingest_weather.py")

    save_status("SUCCESS")

    print("Weather status: SUCCESS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--simulate-failure",
        action="store_true"
    )

    args = parser.parse_args()

    refresh_weather(
        simulate_failure=args.simulate_failure
    )
