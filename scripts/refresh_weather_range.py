
import argparse
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATABASE = ROOT / "data/warehouse/nyc_taxi.duckdb"


def parse_month(value):
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", value):
        raise ValueError("Month must be YYYY-MM")
    return map(int, value.split("-"))


def get_bounds(first, last):
    sy, sm = parse_month(first)
    ey, em = parse_month(last)

    if (sy, sm) > (ey, em):
        raise ValueError("Start must not exceed end")

    begin = datetime(sy, sm, 1, tzinfo=timezone.utc)

    if em == 12:
        ey, em = ey + 1, 1
    else:
        em += 1

    # Include one extra UTC day for New York local hours
    end = datetime(ey, em, 2, tzinfo=timezone.utc)
    return begin, end


def cache_complete(begin, end):
    if not DATABASE.exists():
        return False

    con = duckdb.connect(str(DATABASE), read_only=True)

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

    except duckdb.CatalogException:
        return False

    finally:
        con.close()


def save_status(period, status, error=None):
    con = duckdb.connect(str(DATABASE))

    try:
        con.execute("BEGIN TRANSACTION")
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        con.execute("""
            CREATE TABLE IF NOT EXISTS raw.weather_refresh_status (
                period_id VARCHAR PRIMARY KEY,
                attempted_at_utc TIMESTAMPTZ,
                status VARCHAR,
                error_message VARCHAR
            )
        """)

        con.execute("""
            DELETE FROM raw.weather_refresh_status
            WHERE period_id = ?
        """, [period])

        con.execute("""
            INSERT INTO raw.weather_refresh_status
            VALUES (?, CURRENT_TIMESTAMP, ?, ?)
        """, [period, status, error])

        con.execute("COMMIT")

    except Exception:
        con.execute("ROLLBACK")
        raise

    finally:
        con.close()


def refresh(first, last, simulate_failure=False):
    begin, end = get_bounds(first, last)
    period = f"{first}_to_{last}"

    expected = list(range(
        int(begin.timestamp()),
        int(end.timestamp()),
        3600
    ))

    print(f"Period: {first} to {last}")
    print(f"Expected UTC hours: {len(expected)}")

    if not simulate_failure and cache_complete(begin, end):
        save_status(period, "CACHED_COMPLETE")
        print("Weather cache complete. API skipped.")
        return

    params = {
        "latitude": 40.78,
        "longitude": -73.97,
        "start_date": begin.date().isoformat(),
        "end_date": (end - timedelta(days=1)).date().isoformat(),
        "hourly": "temperature_2m,precipitation,wind_speed_10m",
        "timezone": "GMT",
        "timeformat": "unixtime",
    }

    url = (
        "https://archive-api.open-meteo.com/v1/archive?"
        + urlencode(params)
    )

    error = None
    data = None

    try:
        if simulate_failure:
            raise URLError("Simulated API failure")

        with urlopen(url, timeout=45) as response:
            data = json.load(response)

    except HTTPError as exc:
        if exc.code not in (429, 500, 502, 503, 504):
            raise
        error = str(exc)

    except (URLError, TimeoutError, OSError) as exc:
        error = str(exc)

    if error:
        complete = cache_complete(begin, end)
        status = (
            "API_FAILED_CACHED"
            if complete else "API_FAILED_INCOMPLETE"
        )

        save_status(period, status, error)
        print(f"WARNING: {error}")
        print(f"Weather status: {status}")
        return

    if data.get("error"):
        raise ValueError(f"API error: {data}")

    hourly = data["hourly"]

    if hourly["time"] != expected:
        raise ValueError("Missing or duplicate weather hours")

    variables = [
        "temperature_2m",
        "precipitation",
        "wind_speed_10m",
    ]

    for variable in variables:
        if len(hourly[variable]) != len(expected):
            raise ValueError(f"Incomplete: {variable}")
        if any(v is None for v in hourly[variable]):
            raise ValueError(f"Missing values: {variable}")

    # Preserve the validated raw API response
    raw_file = (
        ROOT / "data/raw" /
        f"weather_{first}_to_{last}.json"
    )
    temporary = raw_file.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(data), encoding="utf-8")
    temporary.replace(raw_file)

    rows = [
        (
            datetime.fromtimestamp(ts, timezone.utc),
            float(hourly["temperature_2m"][i]),
            float(hourly["precipitation"][i]),
            float(hourly["wind_speed_10m"][i]),
        )
        for i, ts in enumerate(expected)
    ]

    con = duckdb.connect(str(DATABASE))

    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw.weather_hourly (
                weather_hour_utc TIMESTAMPTZ PRIMARY KEY,
                temperature_c DOUBLE,
                precipitation_mm DOUBLE,
                wind_speed_kmh DOUBLE
            )
        """)

        con.execute("BEGIN TRANSACTION")

        con.execute("""
            DELETE FROM raw.weather_hourly
            WHERE weather_hour_utc >= ?
              AND weather_hour_utc < ?
        """, [begin, end])

        con.executemany("""
            INSERT INTO raw.weather_hourly
            VALUES (?, ?, ?, ?)
        """, rows)

        con.execute("COMMIT")

    except Exception:
        con.execute("ROLLBACK")
        raise

    finally:
        con.close()

    save_status(period, "SUCCESS")
    print(f"Loaded weather hours: {len(rows)}")
    print("Weather status: SUCCESS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--simulate-failure", action="store_true")

    args = parser.parse_args()

    refresh(
        args.start,
        args.end,
        args.simulate_failure
    )
