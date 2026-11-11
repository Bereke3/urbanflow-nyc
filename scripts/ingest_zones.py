
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[1]

csv_path = ROOT / "data/reference/taxi_zone_lookup.csv"
db_path = ROOT / "data/warehouse/nyc_taxi.duckdb"

if not csv_path.exists():
    raise FileNotFoundError(csv_path)

con = duckdb.connect(str(db_path))

try:
    con.execute("BEGIN TRANSACTION")

    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    con.execute("""
        CREATE OR REPLACE TABLE raw.taxi_zones AS
        SELECT
            CAST(LocationID AS INTEGER) AS location_id,
            Borough AS borough,
            Zone AS zone,
            service_zone
        FROM read_csv_auto(?)
    """, [str(csv_path)])

    total, unique_ids, missing_ids = con.execute("""
        SELECT
            COUNT(*),
            COUNT(DISTINCT location_id),
            COUNT(*) FILTER (WHERE location_id IS NULL)
        FROM raw.taxi_zones
    """).fetchone()

    if total == 0 or total != unique_ids or missing_ids != 0:
        raise ValueError("Invalid taxi zone lookup data")

    con.execute("COMMIT")

    print(f"Total zones: {total}")
    print(f"Unique location IDs: {unique_ids}")
    print("Status: SUCCESS")

except Exception:
    con.execute("ROLLBACK")
    raise
finally:
    con.close()
