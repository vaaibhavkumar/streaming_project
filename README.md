# Real-Time Revenue Reconciliation & Anomaly Detection (Kafka + Spark Structured Streaming)

A small, finance-flavored streaming pipeline built to close a specific gap: hands-on
experience with **Spark Structured Streaming**, **watermarking**, **stateful processing**,
and **Kafka** — as opposed to the batch/micro-batch Event Hub work already on the resume.

## What it does

1. `producer.py` simulates revenue transactions from multiple source systems
   (billing, GL, POS, subscriptions), deliberately injecting:
   - **late-arriving events** (to exercise watermarking / out-of-order handling)
   - **anomalous amounts** (to exercise anomaly detection)
2. `streaming_job.py` consumes the stream and does two things in parallel:
   - **Windowed aggregation with watermarking** — 2-minute tumbling windows of
     revenue per account/source system, tolerating 3 minutes of lateness
     before finalizing a window (`console` sink for live visibility).
   - **True stateful processing** (`applyInPandasWithState`) — maintains a running
     mean per account *across the whole stream*, persisted via checkpointing, and
     flags any transaction that deviates sharply from that account's baseline
     (`parquet` sink for anomaly alerts).

This mirrors typical real-time use cases directly: reconciliation-style
rollups, anomaly-detection signals, and operational reporting built with
watermarking, checkpointing, and stateful streaming.

## Run it locally

```bash
# 1. Start Kafka (single-broker, KRaft mode, no Zookeeper needed)
docker compose up -d

# 2. Create a virtualenv and install deps
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 3. In one terminal: start the producer
python src/producer.py

# 4. In another terminal: start the Spark job
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  src/streaming_job.py
```

You should see windowed aggregate rollups print to console every ~30 seconds,
and anomaly-flagged records land under `streaming_output/anomalies` as parquet.

Stop everything with `Ctrl+C` in both terminals, then `docker compose down`.

## Things worth tweaking to make it "yours" before an interview

- Swap the synthetic producer for a **real public dataset** (e.g. replay a
  Kaggle transactions CSV at intervals) — makes the story less "toy."
- Add a **dead-letter sink** for malformed JSON (ties to the JD's "dead-letter
  queue patterns" bullet).
- Add a **unit test** for `detect_anomalies_per_account` using Spark's
  testing utilities (ties to their "automated testing" ask).
- Swap Kafka for **Azure Event Hub's Kafka-compatible endpoint** if you want
  the exact tool named in the JD — the code barely changes (just the
  bootstrap servers + SASL config).

## Suggested resume bullet (only after you've actually run it and can defend it)

> Built a real-time revenue anomaly-detection pipeline using Kafka and Spark
> Structured Streaming, implementing watermarking for late data, tumbling-window
> revenue rollups, and stateful per-account baseline tracking to flag anomalous
> transactions — closing hands-on gap between prior micro-batch Event Hub work
> and true continuous stream processing.

Keep it framed as a **personal/learning project**, distinct from your paid
work experience — don't blend it into a client engagement bullet. Principal-level
interviewers will ask "whose project was this" and honesty here matters more
than the polish of the bullet.
