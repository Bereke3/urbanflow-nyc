SELECT
    source_month,
    pickup_datetime,
    dropoff_datetime,
    pu_location_id,
    do_location_id,
    passenger_count,
    trip_distance,
    payment_type,
    fare_amount,
    total_amount,

    EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,

    CASE
        WHEN pickup_datetime < dropoff_datetime
         AND source_month = strftime(pickup_datetime, '%Y-%m')
         AND trip_distance > 0
         AND total_amount > 0
         AND pu_location_id > 0
         AND do_location_id > 0
        THEN TRUE
        ELSE FALSE
    END AS is_usable_trip

FROM {{ source('tlc', 'yellow_trips') }}