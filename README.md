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
     before finalizing a window. The live console sink is optional and
     disabled by default in this repo, while durable Parquet output remains
     the primary operational sink.
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

By default, windowed aggregate rollups are written to Parquet files under
`streaming_output/windowed_aggregates`, and anomaly-flagged records land under
`streaming_output/anomalies` as Parquet. Enable live debug output locally with
`--enable-console-debug` when running `spark-submit` if you want terminal
visibility during development.

Stop everything with `Ctrl+C` in both terminals, then `docker compose down`.

## Things worth tweaking to make it "yours"

- Swap the synthetic producer for a **real public dataset** (e.g. replay a
  Kaggle transactions CSV at intervals)
- Add a **dead-letter sink** for malformed JSON (akin to Industry ask of "dead-letter
  queue patterns" bullet).
- Add a **unit test** for `detect_anomalies_per_account` using Spark's
  testing utilities (ties to standard "automated testing" ask).
- Swap Kafka for other similar tools like **Azure Event Hub's Kafka-compatible endpoint** if you want
  — the code barely changes (just the bootstrap servers + SASL config).

## Possible resume bullet (only after you've actually run it and can defend it)

> Built a real-time revenue anomaly-detection pipeline using Kafka and Spark
> Structured Streaming, implementing watermarking for late data, tumbling-window
> revenue rollups, and stateful per-account baseline tracking to flag anomalous
> transactions — closing hands-on gap between prior micro-batch Event Hub work
> and true continuous stream processing.

Note: This is a **personal/learning project**, I have done for learning.

## Project Components

- **Kafka broker:** Local message broker (started via `docker-compose.yml`) that hosts the topic `revenue_transactions` and forwards events from the producer to the streaming job.
- **Producer (file):** `src/producer.py` — simulates and publishes transaction JSON events to Kafka (keys by `account_id`).
- **Stream processor (file):** `src/streaming_job.py` — Spark Structured Streaming application that reads Kafka, computes windowed aggregates, and detects per-account anomalies using stateful processing.
- **Checkpoint directory:** `streaming_checkpoints/` — stores Spark state for recovery; keep local and omit from version control.
- **Output directory:** `streaming_output/` — Parquet files written by the job (windowed aggregates and anomaly alerts); keep local and omit from version control.
- **Docs & tooling:** `README.md`, `issues_fixes_log.md`, and `.gitignore` for instructions, changelog, and ignored runtime artifacts.

## What the producer does (`src/producer.py`)

- **Generates synthetic transactions:** Creates JSON events containing `transaction_id`, `account_id`, `source_system`, `amount`, `currency`, and `event_time`.
- **Injects realism:** Occasionally emits late-arriving events and anomalous amounts to exercise watermarking and detection logic.
- **Publishes to Kafka:** Uses a Kafka producer to send messages to `revenue_transactions`, using `account_id` as the message key to help partitioning.
- **Runtime behavior:** Runs continuously until stopped (Ctrl+C), producing a steady stream of test events for the Spark job to consume.

## What the Spark job does (`src/streaming_job.py`)

- **Consumes Kafka JSON events:** Reads the `revenue_transactions` topic, parses the payload, and converts `event_time` to a timestamp column.
- **Windowed aggregations:** Computes 2-minute tumbling windows with a 3-minute watermark to tolerate late events, producing per-account/source rollups (total amount and transaction count).
- **Stateful anomaly detection:** Maintains per-account running statistics via `applyInPandasWithState` and flags transactions that deviate significantly from the running mean; state is checkpointed for recovery.
- **Sinks:** Persists windowed aggregates and anomaly alerts as Parquet files under `streaming_output/` (append mode). Live console output is optional and disabled by default; enable `ENABLE_CONSOLE_DEBUG` in `src/streaming_job.py` for local development visibility.
- **Checkpointing:** Each sink uses a checkpoint subfolder under `streaming_checkpoints/` — required for exactly-once semantics and stateful recovery.

