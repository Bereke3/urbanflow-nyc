
{{ config(materialized='table') }}

SELECT
    h.pickup_hour,
    h.pu_location_id,
    h.borough,
    h.zone,

    -- Taxi metrics
    h.trip_count,
    h.avg_distance_miles,
    h.avg_total_amount,

    -- Weather metrics
    w.weather_hour_utc,
    w.temperature_c,
    w.precipitation_mm,
    w.wind_speed_kmh,

    CASE
        WHEN w.weather_hour_utc IS NOT NULL
        THEN TRUE
        ELSE FALSE
    END AS weather_available

FROM {{ ref('fct_trips_hourly') }} AS h

LEFT JOIN {{ source('tlc', 'weather_hourly') }} AS w
    ON (
        h.pickup_hour
        AT TIME ZONE 'America/New_York'
    ) = w.weather_hour_utc
