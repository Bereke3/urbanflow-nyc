
import sys
import argparse
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DATABASE = ROOT / "data/warehouse/nyc_taxi.duckdb"
RAW_DIR = ROOT / "data/raw"

FIRST_MONTH = "2025-01"


def months_between(start, end):
    year, month = map(int, start.split("-"))
    end_year, end_month = map(int, end.split("-"))

    while (year, month) <= (end_year, end_month):
        yield f"{year}-{month:02d}"

        month += 1

        if month == 13:
            year += 1
            month = 1


def weather_complete(con, month):
    year, number = map(int, month.split("-"))

    begin = datetime(
        year, number, 1, tzinfo=timezone.utc
    )

    if number == 12:
        next_month = datetime(
            year + 1, 1, 1, tzinfo=timezone.utc
        )
    else:
        next_month = datetime(
            year, number + 1, 1, tzinfo=timezone.utc
        )

    end = next_month + timedelta(days=1)

    try:
        missing = con.execute("""
            SELECT COUNT(*)
            FROM generate_series(
                ?::TIMESTAMPTZ,
                ?::TIMESTAMPTZ - INTERVAL 1 HOUR,
                INTERVAL 1 HOUR
            ) AS expected(hour_utc)

            LEFT JOIN raw.weather_hourly AS w
                ON expected.hour_utc = w.weather_hour_utc

            WHERE w.weather_hour_utc IS NULL
               OR w.temperature_c IS NULL
               OR w.precipitation_mm IS NULL
               OR w.wind_speed_kmh IS NULL
        """, [begin, end]).fetchone()[0]

        return missing == 0

    except duckdb.CatalogException as error:
        print(
            f"Weather validation failed: {month}: {error}",
            file=sys.stderr
        )
        return False


def taxi_complete(con, month):
    filepath = (
        RAW_DIR / f"yellow_tripdata_{month}.parquet"
    )

    if not filepath.is_file():
        print(
            f"Missing source file: {month}",
            file=sys.stderr
        )
        return False

    try:
        source_count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(filepath)]
        ).fetchone()[0]

        loaded_count = con.execute("""
            SELECT COUNT(*)
            FROM raw.yellow_trips
            WHERE source_month = ?
        """, [month]).fetchone()[0]

    except duckdb.Error as error:
        print(
            f"Taxi validation failed: {month}: {error}",
            file=sys.stderr
        )
        return False

    if source_count == 0:
        print(
            f"Empty source file: {month}",
            file=sys.stderr
        )
        return False

    if source_count != loaded_count:
        print(
            f"Row count mismatch: {month}, "
            f"source={source_count}, "
            f"loaded={loaded_count}",
            file=sys.stderr
        )
        return False

    return True


def select_month(latest):
    if latest < FIRST_MONTH:
        return "NONE"

    if not DATABASE.exists():
        return FIRST_MONTH

    con = duckdb.connect(
        str(DATABASE),
        read_only=True
    )

    try:
        for month in months_between(FIRST_MONTH, latest):

            if not taxi_complete(con, month):
                return month

            if not weather_complete(con, month):
                return month

        return "NONE"

    finally:
        con.close()


def valid_month(value):
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", value):
        raise argparse.ArgumentTypeError(
            "Expected month format: YYYY-MM"
        )

    return value


def main():
    parser = argparse.ArgumentParser(
        description="Find the first incomplete NYC Taxi month"
    )

    parser.add_argument(
        "--through",
        required=True,
        type=valid_month,
        help="Latest eligible month in YYYY-MM format",
    )

    args = parser.parse_args()

    selected_month = select_month(args.through)

    # Only the selected month goes to stdout.
    # Airflow reads this value in prepare_period.
    print(selected_month, flush=True)


if __name__ == "__main__":
    main()
