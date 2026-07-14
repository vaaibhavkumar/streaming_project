"""
Synthetic revenue-transaction producer.

Simulates a finance/billing event stream: each event is a transaction posted
by one of a handful of source systems (mimicking multi-source finance
ingestion, like the JD's "billing, revenue, GL, opex" language).

Deliberately injects:
  - occasional LATE events (timestamp a few minutes in the past) to exercise
    Spark's watermarking / late-data handling
  - occasional ANOMALOUS amounts (very large, or negative where not expected)
    to exercise the anomaly-detection logic in the consumer

Run:
    pip install kafka-python
    python producer.py
"""
import json
import random
import time
import uuid
from datetime import datetime, timedelta

from kafka import KafkaProducer

TOPIC = "revenue_transactions"
BOOTSTRAP_SERVERS = "localhost:9092"

SOURCE_SYSTEMS = ["billing_sys", "gl_sys", "pos_sys", "subscriptions_sys"]
ACCOUNTS = [f"ACC-{i:04d}" for i in range(1, 21)]


def make_event(late: bool = False, anomalous: bool = False) -> dict:
    now = datetime.utcnow()
    # Set the transaction time: if it's marked as "late", artificially delay it by 3 to 8 minutes 
    # to test how the system handles lag; otherwise, use the exact current time.
    event_time = now - timedelta(minutes=random.randint(3, 8)) if late else now

    amount = round(random.uniform(10, 500), 2)
    if anomalous:
        # simulate a spurious/duplicate-looking spike or an unexpected refund
        amount = round(random.choice([random.uniform(5000, 20000), -random.uniform(1000, 5000)]), 2)

    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": random.choice(ACCOUNTS),
        "source_system": random.choice(SOURCE_SYSTEMS),
        "amount": amount,
        "currency": "USD",
        "event_time": event_time.isoformat(),
    }


def main():
    # Set up the data sender (Kafka Producer) to package our transaction data 
    # into a standard text format (JSON) so it can be securely and cleanly transmitted over the network.
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    print(f"Producing to topic '{TOPIC}' on {BOOTSTRAP_SERVERS} — Ctrl+C to stop")
    try:
        while True:
            # random.random() generates a random decimal number (a floating-point number) between 0.0 and 1.0.
            # Specifically, the number it picks will be greater than or equal to 0.0, but strictly less than 1.0 
            # (written mathematically as $[0.0, 1.0)$)
            late = random.random() < 0.10       # ~10% late-arriving events
            anomalous = random.random() < 0.05  # ~5% anomalous amounts

            event = make_event(late=late, anomalous=anomalous)
            producer.send(TOPIC, key=event["account_id"], value=event)

            tag = " (LATE)" if late else " (ANOMALY)" if anomalous else ""
            print(f"sent{tag}: {event}")

            time.sleep(random.uniform(0.2, 1.0))
    except KeyboardInterrupt:
        print("Stopping producer.")
    finally:
        # To maximize speed, the Kafka producer doesn't immediately send every single transaction over the network 
        # the exact millisecond it's created. Instead, it temporarily saves them in a tiny background memory buffer
        producer.flush() # Empty the bucket completely.
        # Keeping a connection open to a database or a Kafka broker consumes network resources (like memory and open sockets). 
        # This line cleanly disconnects script from the Kafka broker, freeing up those resources so the system stays efficient.
        producer.close() # Hang up the phone safely.


if __name__ == "__main__":
    main()
