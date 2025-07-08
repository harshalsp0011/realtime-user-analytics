# producer.py
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Sample configuration (adjust if needed)
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'user_events'

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Biased event type distribution (60% click, 30% cart, 10% purchase)
def weighted_event_type():
    return random.choices(
        population=['click', 'add_to_cart', 'purchase'],
        weights=[0.6, 0.3, 0.1],
        k=1
    )[0]

# Main loop: generate and send events
def generate_event():
    event = {
        "user_id": random.randint(1, 100),
        "product_id": random.randint(1, 500),
        "event_type": weighted_event_type(),
        "timestamp": datetime.utcnow().isoformat()
    }
    return event

if __name__ == "__main__":
    print(f"Sending events to Kafka topic '{TOPIC_NAME}'...")
    try:
        while True:
            event = generate_event()
            producer.send(TOPIC_NAME, value=event)
            print("Event sent:", event)
            time.sleep(1)  # Send 1 event per second
    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        producer.close()
