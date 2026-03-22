import json
import pandas as pd
from kafka import KafkaProducer
from time import time

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"

columns = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount',
]

print("Reading parquet file...")
df = pd.read_parquet(url, columns=columns)
print(f"Loaded {len(df)} rows")

# Convert datetime columns to strings
for col in ['lpep_pickup_datetime', 'lpep_dropoff_datetime']:
    df[col] = df[col].astype(str)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

t0 = time()

for _, row in df.iterrows():
    producer.send('green-trips', value=row.to_dict())

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
