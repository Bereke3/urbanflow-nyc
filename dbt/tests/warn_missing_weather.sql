
{{ config(severity='warn') }}

SELECT
    pickup_hour,
    pu_location_id
FROM {{ ref('fct_taxi_weather_hourly') }}
WHERE weather_available = FALSE
