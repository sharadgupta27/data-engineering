import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer, TopicPartition
from models import ride_deserializer

BOOTSTRAP_SERVER = 'localhost:9092'
TOPIC_NAME = 'green-trips'
DISTANCE_THRESHOLD = 5.0

# No group_id / no committed offsets — manually assign & seek to beginning
# so stale offsets from a previous run (or a recreated topic) never interfere.
consumer = KafkaConsumer(
    bootstrap_servers=[BOOTSTRAP_SERVER],
    auto_offset_reset='earliest',
    value_deserializer=ride_deserializer,
    consumer_timeout_ms=10000,  # stop after 10s of no new messages
)

partitions = consumer.partitions_for_topic(TOPIC_NAME)
if not partitions:
    print(f"ERROR: topic '{TOPIC_NAME}' not found or has no partitions.")
    consumer.close()
    sys.exit(1)

topic_partitions = [TopicPartition(TOPIC_NAME, p) for p in sorted(partitions)]
consumer.assign(topic_partitions)
consumer.seek_to_beginning(*topic_partitions)

print(f"Reading all messages from '{TOPIC_NAME}' (seeked to beginning, {len(topic_partitions)} partition(s))...")

total = 0
long_trips = 0

for message in consumer:
    ride = message.value
    total += 1
    if ride.trip_distance > DISTANCE_THRESHOLD:
        long_trips += 1

consumer.close()

print(f"\nTotal trips consumed : {total}")
print(f"Trips with trip_distance > {DISTANCE_THRESHOLD} : {long_trips}")
