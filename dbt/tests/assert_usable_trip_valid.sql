
SELECT
    source_month,
    pickup_datetime,
    dropoff_datetime,
    trip_distance,
    total_amount

FROM {{ ref('stg_trips') }}

WHERE is_usable_trip = TRUE
  AND (
      trip_distance <= 0
      OR total_amount <= 0
      OR pickup_datetime >= dropoff_datetime
      OR source_month <> strftime(pickup_datetime, '%Y-%m')
  )
