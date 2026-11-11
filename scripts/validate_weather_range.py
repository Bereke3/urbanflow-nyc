
import argparse
import sys

import duckdb

from select_backfill_month import (
    DATABASE,
    months_between,
    weather_complete,
)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)

    args = parser.parse_args()

    if args.start > args.end:
        parser.error("Start month must not exceed end month")

    if not DATABASE.exists():
        print("Warehouse database not found", file=sys.stderr)
        sys.exit(1)

    con = duckdb.connect(
        str(DATABASE),
        read_only=True,
    )

    try:
        for month in months_between(args.start, args.end):
            if not weather_complete(con, month):
                print(
                    f"WEATHER_INCOMPLETE: {month}",
                    file=sys.stderr,
                )
                sys.exit(1)

            print(f"WEATHER_COMPLETE: {month}")

    finally:
        con.close()

    print("WEATHER VALIDATION: SUCCESS")


if __name__ == "__main__":
    main()
