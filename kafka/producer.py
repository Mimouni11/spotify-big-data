import json
import glob
import sys
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "spotify-personal"
DATA_DIR = "Spotify Extended Streaming History"

print("Connecting to Kafka...")
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
except NoBrokersAvailable:
    print("ERROR: Cannot connect to Kafka. Is it running?")
    sys.exit(1)

# Find all audio JSON files
files = sorted(glob.glob(f"{DATA_DIR}/Streaming_History_Audio_*.json"))
if not files:
    print(f"ERROR: No Streaming_History_Audio_*.json files found in {DATA_DIR}/")
    sys.exit(1)

print(f"Found {len(files)} files")

total_sent = 0
for filepath in files:
    filename = filepath.split("/")[-1].split("\\")[-1]
    with open(filepath, "r", encoding="utf-8") as f:
        events = json.load(f)

    print(f"  Streaming {filename} ({len(events)} events)...")
    for event in events:
        producer.send(TOPIC, event)
        total_sent += 1
        if total_sent % 5000 == 0:
            print(f"    {total_sent} events sent...")

    producer.flush()

print(f"Done! Streamed {total_sent} events to topic '{TOPIC}'")
producer.close()
