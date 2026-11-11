
{{ config(materialized='table') }}

WITH hourly_trips AS (

    SELECT
        DATE_TRUNC('hour', pickup_datetime) AS pickup_hour,
        pu_location_id,

        COUNT(*) AS trip_count,

        ROUND(AVG(trip_distance), 2)
            AS avg_distance_miles,

        ROUND(AVG(total_amount), 2)
            AS avg_total_amount

    FROM {{ ref('stg_trips') }}

    WHERE is_usable_trip = TRUE

    GROUP BY 1, 2
)

SELECT
    h.pickup_hour,
    h.pu_location_id,
    z.borough,
    z.zone,
    h.trip_count,
    h.avg_distance_miles,
    h.avg_total_amount

FROM hourly_trips AS h

LEFT JOIN {{ source('tlc', 'taxi_zones') }} AS z
    ON h.pu_location_id = z.location_id
