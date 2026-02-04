## Query 1: Count Short Distance Trips (≤ 1 mile)

**Purpose:** Count the total number of green taxi trips with a distance of 1 mile or less.

**Query:**
```sql
SELECT 
	COUNT(1) 
FROM 
	green_taxi_trips_nov2025
WHERE "trip_distance" <= 1;
```

**Explanation:**
- `COUNT(1)`: Counts all rows matching the filter condition
- Filters for trips where `trip_distance` is less than or equal to 1 mile
- Useful for analyzing short-distance trip patterns

---

## Query 2: Maximum Trip Distance by Day

**Purpose:** Find the maximum trip distance for each day in November 2025, excluding outliers.

**Query:**
```sql
SELECT 
    CAST(lpep_pickup_datetime AS DATE) as "day",
    MAX("trip_distance") as "Max Distance"
FROM 
    green_taxi_trips_nov2025
WHERE "trip_distance" < 100
GROUP BY CAST(lpep_pickup_datetime AS DATE)
ORDER BY MAX("trip_distance") DESC;
```

**Explanation:**
- `CAST(lpep_pickup_datetime AS DATE)`: Extracts the date portion from the pickup timestamp
- `MAX("trip_distance")`: Finds the longest trip for each day
- `WHERE "trip_distance" < 100`: Filters out unrealistic distances (likely data errors)
- Results are grouped by day and sorted by maximum distance in descending order
- Helps identify days with unusually long trips

---

## Query 3: Total Revenue by Pickup Location for Specific Date

**Purpose:** Calculate total revenue by pickup location for trips on November 18, 2025.

**Query:**
```sql
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
```

**Explanation:**
- Joins green taxi trip data with zone lookup table to get location names
- `CONCAT(zpu."Borough", ' / ', zpu."Zone")`: Creates readable location format (e.g., "Manhattan / Times Square")
- `SUM(total_amount)`: Aggregates total revenue for each pickup location
- Filters for specific date: November 18, 2025
- `GROUP BY 1, 2, 4`: Groups by day, location ID, and location name
- Results sorted by total revenue in descending order
- Identifies the most profitable pickup locations on that date

---

## Query 4: Largest Tip from East Harlem North

**Purpose:** Find the drop-off location that received the largest tip from trips originating in East Harlem North.

**Query:**
```sql
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
```

**Explanation:**
- Uses two joins with the zones table:
  - `zpu`: For pickup location information
  - `zdo`: For drop-off location information
- `WHERE zpu."Zone" = 'East Harlem North'`: Filters to only trips picked up in East Harlem North
- `MAX(tip_amount)`: Finds the highest tip for each drop-off location
- `GROUP BY zdo."Zone", zdo."Borough"`: Aggregates by drop-off location
- `LIMIT 1`: Returns only the top result with the largest tip
- Helps identify which destinations from East Harlem North generate the highest tips