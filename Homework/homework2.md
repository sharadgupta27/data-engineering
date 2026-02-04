**Question 3:** How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?

**Database:** BigQuery (`kestra-dataengg.zoomcamp.yellow_tripdata`)

**Query:**
```sql
SELECT
  COUNT(unique_row_id)
FROM
  `kestra-dataengg`.`zoomcamp`.`yellow_tripdata`
WHERE
  TIMESTAMP_TRUNC(tpep_pickup_datetime, YEAR) = TIMESTAMP '2020-01-01 00:00:00 UTC';
```

**Explanation:**
- `COUNT(unique_row_id)`: Counts all records with unique row identifiers
- `TIMESTAMP_TRUNC(tpep_pickup_datetime, YEAR)`: Truncates the pickup datetime to the year level
- Filter condition matches all trips where the pickup datetime falls within 2020
- Uses BigQuery syntax for timestamp comparison

---

**Question 4:** How many rows are there for the Green Taxi data for all CSV files in the year 2020?

**Database:** BigQuery (`kestra-dataengg.zoomcamp.green_tripdata`)

**Query:**
```sql
SELECT
  COUNT(unique_row_id)
FROM
  `kestra-dataengg`.`zoomcamp`.`green_tripdata`
WHERE
  TIMESTAMP_TRUNC(lpep_pickup_datetime, YEAR) = TIMESTAMP '2020-01-01 00:00:00 UTC';
```

**Explanation:**
- `COUNT(unique_row_id)`: Counts all records with unique row identifiers
- `TIMESTAMP_TRUNC(lpep_pickup_datetime, YEAR)`: Truncates the pickup datetime to the year level
- Filter condition matches all trips where the pickup datetime falls within 2020
- Uses BigQuery syntax with backtick notation for table reference
- `lpep_pickup_datetime` is specific to green taxi data (vs `tpep_pickup_datetime` for yellow taxis)

**Key Differences Between Queries:**
- **Taxi Type**: Query 3 analyzes Yellow taxi data, Query 4 analyzes Green taxi data
- **Pickup Datetime Column**: Yellow taxis use `tpep_pickup_datetime`, green taxis use `lpep_pickup_datetime`
- **Table Reference**: Both queries use BigQuery with the same database and schema path, different table names