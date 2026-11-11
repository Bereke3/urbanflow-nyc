
import argparse
import shutil
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import duckdb

from ingest_taxi_batch import month_range

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/raw"

BASE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/"
    "trip-data"
)

REQUIRED_COLUMNS = {
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "total_amount",
}


def validate_parquet(path):
    con = duckdb.connect()

    try:
        result = con.execute(
            "SELECT * FROM read_parquet(?) LIMIT 0",
            [str(path)]
        )

        columns = {
            column[0]
            for column in result.description
        }

        missing = REQUIRED_COLUMNS - columns

        if missing:
            raise ValueError(
                f"Missing columns: {sorted(missing)}"
            )

        count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(path)]
        ).fetchone()[0]

        if count == 0:
            raise ValueError("Parquet file is empty")

        return count

    finally:
        con.close()


class TLCNotPublishedError(Exception):
    pass


def download_month(month):
    RAW.mkdir(parents=True, exist_ok=True)

    filename = f"yellow_tripdata_{month}.parquet"
    destination = RAW / filename
    temporary = RAW / f"{filename}.part"

    if destination.exists():
        try:
            count = validate_parquet(destination)
            print(f"{month}: cached, {count:,} rows")
            return
        except (duckdb.Error, ValueError):
            print(f"{month}: invalid local file, downloading")

    url = f"{BASE_URL}/{filename}"

    try:
        for attempt in range(3):
            try:
                request = Request(
                    url,
                    headers={"User-Agent": "NYCTaxiDE/1.0"}
                )

                with urlopen(request, timeout=90) as response:
                    with temporary.open("wb") as output:
                        shutil.copyfileobj(response, output)

                count = validate_parquet(temporary)
                temporary.replace(destination)

                print(f"{month}: downloaded {count:,} rows")
                return

            except HTTPError as error:       
                if error.code == 404:
                    raise TLCNotPublishedError(
                        f"TLC data not published: {month}"
                    ) from error


                if error.code == 429 or error.code < 500:
                    raise

                if attempt == 2:
                    raise

            except (URLError, TimeoutError):
                if attempt == 2:
                    raise

            time.sleep(2 ** attempt)

    finally:
        temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)

    args = parser.parse_args()

    
    try:
        for month in month_range(args.start, args.end):
            download_month(month)

    except TLCNotPublishedError as exc:
        print(f"TLC_NOT_PUBLISHED: {exc}", flush=True)
        raise SystemExit(75)

    print("TAXI DOWNLOAD: SUCCESS")

