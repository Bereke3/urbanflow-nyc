
WITH expected AS (
    SELECT
        COUNT(*) AS row_count,
        SUM(trip_count) AS trip_count
    FROM {{ ref('fct_trips_hourly') }}
),

actual AS (
    SELECT
        COUNT(*) AS row_count,
        SUM(trip_count) AS trip_count
    FROM {{ ref('fct_taxi_weather_hourly') }}
)

SELECT
    expected.row_count AS expected_rows,
    actual.row_count AS actual_rows,
    expected.trip_count AS expected_trips,
    actual.trip_count AS actual_trips
FROM expected
CROSS JOIN actual
WHERE expected.row_count <> actual.row_count
   OR expected.trip_count <> actual.trip_count
