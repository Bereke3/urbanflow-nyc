
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]

SOURCE = ROOT / "data/raw/weather_nyc_2025_q1.json"
DATABASE = ROOT / "data/warehouse/nyc_taxi.duckdb"

# Expected period in UTC
START = datetime(2025, 1, 1, tzinfo=timezone.utc)
END = datetime(2025, 4, 2, tzinfo=timezone.utc)


def ingest_weather():
    with open(SOURCE, encoding="utf-8") as f:
        data = json.load(f)

    hourly = data["hourly"]
    timestamps = hourly["time"]

    expected = list(
        range(int(START.timestamp()), int(END.timestamp()), 3600)
    )

    if timestamps != expected:
        raise ValueError("Incomplete or duplicate weather hours")

    variables = [
        "temperature_2m",
        "precipitation",
        "wind_speed_10m",
    ]

    for variable in variables:
        if len(hourly[variable]) != len(timestamps):
            raise ValueError(f"Invalid length: {variable}")

    rows = []

    for i, timestamp in enumerate(timestamps):
        temperature = hourly["temperature_2m"][i]
        precipitation = hourly["precipitation"][i]
        wind = hourly["wind_speed_10m"][i]

        if any(v is None for v in [temperature, precipitation, wind]):
            raise ValueError(f"Missing weather value at index {i}")

        rows.append((
            datetime.fromtimestamp(timestamp, timezone.utc),
            temperature,
            precipitation,
            wind,
        ))

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

        try:
            # Replace existing hours to prevent duplicates
            con.execute("""
                DELETE FROM raw.weather_hourly
                WHERE weather_hour_utc >= ?
                  AND weather_hour_utc < ?
            """, [START, END])

            con.executemany("""
                INSERT INTO raw.weather_hourly
                VALUES (?, ?, ?, ?)
            """, rows)

            loaded = con.execute("""
                SELECT COUNT(*)
                FROM raw.weather_hourly
                WHERE weather_hour_utc >= ?
                  AND weather_hour_utc < ?
            """, [START, END]).fetchone()[0]

            if loaded != len(rows):
                raise ValueError("Weather row count mismatch")

            con.execute("COMMIT")

        except Exception:
            con.execute("ROLLBACK")
            raise

        print(f"Source hours: {len(rows)}")
        print(f"Loaded hours: {loaded}")
        print("Status: SUCCESS")

    finally:
        con.close()


if __name__ == "__main__":
    ingest_weather()
