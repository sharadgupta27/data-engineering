import dataclasses
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from kafka import KafkaProducer
from models import Ride, ride_from_row

# Download NYC yellow taxi trip data (first 1000 rows)
# url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"

columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount']
# columns = ['PULocationID', 'DOLocationID', 'trip_distance', 'total_amount', 'tpep_pickup_datetime']
df = pd.read_parquet(url, columns=columns)#.head(1000)

def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    # Replace float NaN with None so it serializes as JSON null
    cleaned = {k: (None if isinstance(v, float) and v != v else v) for k, v in ride_dict.items()}
    json_str = json.dumps(cleaned)
    return json_str.encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer
)
t0 = time.time()

# topic_name = 'rides'
topic_name = 'green_trips'

total = len(df)
for i, (_, row) in enumerate(df.iterrows()):
    ride = ride_from_row(row)
    producer.send(topic_name, value=ride)
    if i % 1000 == 0:
        print(f"Sent {i}/{total}...")

producer.flush()
print(f"Done. Sent {total} messages.")

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
