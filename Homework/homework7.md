# Module 7 Homework: Streaming with Kafka (Redpanda) and PyFlink

## Question 1. Redpanda version

Run `rpk version` inside the Redpanda container:

```bash
docker exec -it workshop-redpanda-1 rpk version
```

What version of Redpanda are you running?

### Answer: **v25.3.29**

---

## Question 2. Sending data to Redpanda

Create a topic called `green-trips` and write a producer to send the green taxi data to this topic, keeping only these columns: `lpep_pickup_datetime`, `lpep_dropoff_datetime`, `PULocationID`, `DOLocationID`, `passenger_count`, `trip_distance`, `tip_amount`, `total_amount`.

Measure the time it takes to send the entire dataset and flush:

```python
from time import time

t0 = time()

# send all rows ...

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
```

How long did it take to send the data?

- 10 seconds
- 60 seconds
- 120 seconds
- 300 seconds

### Answer: **10 seconds (actual time: ~7.33 seconds)**

```bash
docker compose up --build -d
docker exec -it workshop-redpanda-1 rpk topic create green-trips --partitions 1 --replicas 1
uv run python src/producers/producer_green_trips.py
```

---

## Question 3. Consumer - trip distance

Write a Kafka consumer that reads all messages from the `green-trips` topic
(set `auto_offset_reset='earliest'`).

Count how many trips have a `trip_distance` greater than 5.0 kilometers.

How many trips have `trip_distance` > 5?

- 6506
- 7506
- 8506
- 9506

### Answer: **8506**

```bash
docker compose up --build -d
docker exec -it workshop-redpanda-1 rpk topic create green-trips --partitions 1 --replicas 1
uv run python src/producers/producer.py
uv run python src/consumers/consumer_count_trips.py
``` 

---

## Question 4. Tumbling window - pickup location

Create a Flink job that reads from `green-trips` and uses a 5-minute tumbling window to count trips per `PULocationID`.

Write the results to a PostgreSQL table with columns: `window_start`, `PULocationID`, `num_trips`.

After the job processes all data, query the results:

```sql
SELECT PULocationID, num_trips
FROM <your_table>
ORDER BY num_trips DESC
LIMIT 3;
```

Which `PULocationID` had the most trips in a single 5-minute window?

- 42
- 74
- 75
- 166

### Answer: **74**

```bash
# 1. Start the stack
cd workshop
docker compose up --build -d

# 2. Create the Kafka topic
docker exec workshop-redpanda-1 rpk topic create green-trips --partitions 1 --replicas 1

# 3. Create the PostgreSQL sink table
PGPASSWORD=postgres docker compose exec postgres psql -U postgres -d postgres -c "
DROP TABLE IF EXISTS processed_events_windowed;
CREATE TABLE processed_events_windowed (
    window_start TIMESTAMP,
    pulocationid INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, pulocationid)
);";

# 4. Produce all green taxi data into Kafka
uv run python src/producers/producer.py

# 5. Submit the Flink tumbling window job
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/tumbling_window_job.py

# 6. Query the results
PGPASSWORD=postgres docker compose exec postgres psql -U postgres -d postgres -c "
SELECT pulocationid AS \"PULocationID\", num_trips
FROM processed_events_windowed
ORDER BY num_trips DESC
LIMIT 3;"
```

```
window_start          | pulocationid | num_trips
2025-10-01 00:00:00   |     74       |    15
2025-10-01 00:00:00   |     75       |     8
```

---

## Question 5. Session window - longest streak

Create another Flink job that uses a session window with a 5-minute gap on `PULocationID`, using `lpep_pickup_datetime` as the event time with a 5-second watermark tolerance.

Write the results to a PostgreSQL table and find the `PULocationID` with the longest session (most trips in a single session).

How many trips were in the longest session?

- 12
- 31
- 51
- 81

### Answer: **81**

```bash
# 1. Create the PostgreSQL sink table
PGPASSWORD=postgres docker compose exec postgres psql -U postgres -d postgres -c "
DROP TABLE IF EXISTS processed_events_session;
CREATE TABLE processed_events_session (
    window_start TIMESTAMP,
    window_end   TIMESTAMP,
    pulocationid INT,
    num_trips    BIGINT,
    PRIMARY KEY (window_start, window_end, pulocationid)
);"

# 2. Produce all 49,416 green taxi rows into Kafka
uv run python src/producers/producer.py

# 3. Send a sentinel message to advance the watermark
uv run python3 -c "
import json
from kafka import KafkaProducer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
producer.send('green_trips', value={
    'PULocationID': 0, 'DOLocationID': 0,
    'passenger_count': 0.0, 'trip_distance': 0.0,
    'tip_amount': 0.0, 'total_amount': 0.0,
    'lpep_pickup_datetime': '2025-12-01 00:00:00',
    'lpep_dropoff_datetime': '2025-12-01 00:00:00'
})
producer.flush()
print('Sentinel sent')
"

# 4. Submit the session window Flink job
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/session_window_job.py

# 5. Query the results
SELECT pulocationid, num_trips, window_start, window_end
FROM processed_events_session
WHERE pulocationid != 0
ORDER BY num_trips DESC
LIMIT 3;
```

```
+--------------+-----------+---------------------+---------------------+
| pulocationid | num_trips | window_start        | window_end          |
|--------------+-----------+---------------------+---------------------|
| 74           | 81        | 2025-10-08 06:46:14 | 2025-10-08 08:27:40 |
| 74           | 72        | 2025-10-01 06:52:23 | 2025-10-01 08:23:33 |
| 74           | 71        | 2025-10-22 06:58:31 | 2025-10-22 08:25:04 |
+--------------+-----------+---------------------+---------------------+
```

---

## Question 6. Tumbling window - largest tip

Create a Flink job that uses a 1-hour tumbling window to compute the total `tip_amount` per hour (across all locations).

Which hour had the highest total tip amount?

- 2025-10-01 18:00:00
- 2025-10-16 18:00:00
- 2025-10-22 08:00:00
- 2025-10-30 16:00:00

### Answer: **2025-10-16 18:00:00**

```bash
# 1. Create the PostgreSQL sink table
PGPASSWORD=postgres docker compose exec -T postgres psql -U postgres -d postgres -c "
DROP TABLE IF EXISTS processed_events_hourly_tips;
CREATE TABLE processed_events_hourly_tips (
    window_start TIMESTAMP,
    total_tip_amount DOUBLE PRECISION,
    PRIMARY KEY (window_start)
);"

# 2. Produce all green taxi data to Kafka
uv run python src/producers/producer.py

# 3. Send a sentinel message to advance the Flink watermark
uv run python3 -c "
import json
from kafka import KafkaProducer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
sentinel = {
    'lpep_pickup_datetime': '2025-12-01 00:00:00',
    'lpep_dropoff_datetime': '2025-12-01 00:01:00',
    'PULocationID': 0, 'DOLocationID': 0,
    'passenger_count': 0.0, 'trip_distance': 0.0,
    'tip_amount': 0.0, 'total_amount': 0.0
}
producer.send('green_trips', sentinel)
producer.flush()
print('Sentinel sent')
"

# 4. Submit the Flink job
docker exec -d workshop-jobmanager-1 flink run -py /opt/src/job/hourly_tips_job.py

# 5. Query the top hours
SELECT window_start, total_tip_amount
FROM processed_events_hourly_tips
ORDER BY total_tip_amount DESC
LIMIT 5;
```

```
+---------------------+--------------------+
| window_start        | total_tip_amount   |
|---------------------+--------------------|
| 2025-10-16 18:00:00 | 524.96             |
| 2025-10-30 16:00:00 | 507.10             |
| 2025-10-10 17:00:00 | 499.60             |
| 2025-10-09 18:00:00 | 482.96             |
| 2025-10-16 17:00:00 | 463.73             |
+---------------------+--------------------+
```