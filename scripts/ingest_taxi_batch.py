
import argparse
import re
from pathlib import Path

from ingest_taxi import ingest

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/raw"


def validate_month(month):
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month):
        raise ValueError(f"Invalid month: {month}")

    year, month_number = map(int, month.split("-"))
    return year, month_number


def month_range(start, end):
    year, month = validate_month(start)
    end_year, end_month = validate_month(end)

    if (year, month) > (end_year, end_month):
        raise ValueError("Start month must be before end month")

    months = []

    while (year, month) <= (end_year, end_month):
        months.append(f"{year}-{month:02d}")

        month += 1

        if month == 13:
            month = 1
            year += 1

    return months


def ingest_batch(start, end):
    months = month_range(start, end)

    # Validate all files before changing the database
    for month in months:
        filepath = RAW / f"yellow_tripdata_{month}.parquet"

        if not filepath.is_file():
            raise FileNotFoundError(
                f"Missing parquet file: {filepath}"
            )

    print(f"Processing {len(months)} months")

    for month in months:
        print(f"\nProcessing month: {month}")
        ingest(month)

    print("\nBATCH INGESTION: SUCCESS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)

    args = parser.parse_args()

    ingest_batch(args.start, args.end)
