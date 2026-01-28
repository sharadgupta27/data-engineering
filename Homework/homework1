SELECT 
	COUNT(1) 
FROM 
	green_taxi_trips_nov2025
WHERE "trip_distance" <= 1;

========================================================

SELECT 
    CAST(lpep_pickup_datetime AS DATE) as "day",
    MAX("trip_distance") as "Max Distance"
FROM 
    green_taxi_trips_nov2025
WHERE "trip_distance" < 100
GROUP BY CAST(lpep_pickup_datetime AS DATE)
ORDER BY MAX("trip_distance") DESC;

=============================================================

SELECT
    CAST(lpep_pickup_datetime AS DATE) as "day",
    "PULocationID",
    SUM(total_amount),
    CONCAT(zpu."Borough", ' / ', zpu."Zone") AS "pickup_loc"
FROM
    green_taxi_trips_nov2025 gt
    JOIN zones zpu
    ON gt."PULocationID" = zpu."LocationID"
WHERE
    CAST(lpep_pickup_datetime AS DATE) = '2025-11-18'
GROUP BY
    1, 2, 4
ORDER BY 
    SUM(total_amount) DESC;

===========================================================

SELECT
    CONCAT(zdo."Borough", ' / ', zdo."Zone") AS "dropoff_loc",
    MAX(tip_amount) as "largest tip"
FROM
    green_taxi_trips_nov2025 gt
    JOIN zones zpu 
    ON gt."PULocationID" = zpu."LocationID"
    JOIN zones zdo
    ON gt."DOLocationID" = zdo."LocationID"
WHERE
    zpu."Zone" = 'East Harlem North'
GROUP BY
    zdo."Zone", zdo."Borough"
ORDER BY
    "largest tip" DESC
LIMIT 1;