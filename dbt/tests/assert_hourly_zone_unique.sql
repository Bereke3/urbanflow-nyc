
SELECT
    pickup_hour,
    pu_location_id,
    COUNT(*) AS duplicate_count

FROM {{ ref('fct_trips_hourly') }}

GROUP BY 1, 2

HAVING COUNT(*) > 1
