
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]

OUTPUT = ROOT / "data/raw/weather_nyc_2025_q1.json"

params = {
    "latitude": 40.78,
    "longitude": -73.97,
    "start_date": "2025-01-01",
    "end_date": "2025-04-01",
    "hourly": (
        "temperature_2m,"
        "precipitation,"
        "wind_speed_10m"
    ),
    "timezone": "GMT",
    "timeformat": "unixtime",
}

url = (
    "https://archive-api.open-meteo.com/v1/archive?"
    + urlencode(params)
)

# Retry failed API requests up to three times
for attempt in range(3):
    try:
        with urlopen(url, timeout=45) as response:
            data = json.load(response)
        break

    except (OSError, TimeoutError, ValueError) as error:
        print(f"API attempt {attempt + 1} failed: {error}")

        if attempt == 2:
            raise RuntimeError("Weather download failed") from error

        time.sleep(2 ** attempt)

if data.get("error"):
    raise RuntimeError(f"Open-Meteo error: {data}")

hourly = data["hourly"]
timestamps = hourly["time"]

start = int(
    datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp()
)
end = int(
    datetime(2025, 4, 2, tzinfo=timezone.utc).timestamp()
)

expected_timestamps = list(range(start, end, 3600))

# Validate completeness and timestamp uniqueness
if timestamps != expected_timestamps:
    raise ValueError("Missing, duplicated, or unordered weather hours")

variables = [
    "temperature_2m",
    "precipitation",
    "wind_speed_10m",
]

for variable in variables:
    if len(hourly[variable]) != len(timestamps):
        raise ValueError(f"Incomplete variable: {variable}")

# Save validated API response
OUTPUT.parent.mkdir(parents=True, exist_ok=True)

temporary_file = OUTPUT.with_suffix(".json.tmp")
temporary_file.write_text(json.dumps(data), encoding="utf-8")
temporary_file.replace(OUTPUT)

print("Weather download: SUCCESS")
print(f"Hourly records: {len(timestamps)}")
print(f"Saved to: {OUTPUT}")
