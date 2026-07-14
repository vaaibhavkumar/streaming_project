"""
Real-time revenue reconciliation & anomaly detection — Spark Structured Streaming.

Demonstrates, end to end, the concepts the JD calls out under
STREAMING & REAL-TIME:
  - Kafka source ingestion
  - watermarking for late/out-of-order data
  - tumbling-window stateful aggregation (per account/source system)
  - arbitrary stateful processing (applyInPandasWithState) to maintain a
    running per-account baseline and flag anomalous transactions —
    a simplified stand-in for the JD's "reconciliation, anomaly detection
    signals, operational reporting" use case
  - checkpointing for fault tolerance / exactly-once-ish restart behavior

Run (after producer.py is running and Kafka is up):
    pip install pyspark==3.5.1
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 streaming_job.py
"""
from datetime import datetime

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as _sum, count as _count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.streaming.state import GroupStateTimeout

"""
In a large enterprise environment, a Kafka setup (cluster) might consist of dozens or hundreds of connected computers (called brokers).
Instead of forcing your code to list out every single one of those hundred servers, Kafka uses a bootstrap pattern:
- Your script only needs to know one valid, working broker to start with (the "bootstrap" server).
- The moment your code connects to localhost:9092, that broker hands your script a complete, up-to-date map of the entire network.
- This way, your code can immediately start sending and receiving messages to/from the right brokers without needing to know all of them in advance.
- local project only uses a single broker right now, the KAFKA_BOOTSTRAP configuration is what allows the system to establish that crucial first handshake and discover where to send or read data.
"""
KAFKA_BOOTSTRAP = "localhost:9092"

# If Kafka is a massive, real-time postal system, and a Topic as a specific mailbox.
# Without this shared identifier, the data sender and the data processor wouldn't know where to find each other.
#2. Organizing by Business Domain
# In a real enterprise environment, a single Kafka cluster might handle thousands of different data streams simultaneously (e.g., user_logins, inventory_updates, clickstream_events).
# By explicitly naming this topic "revenue_transactions", you are separating financial data from other system noise. This ensures your Spark processing engine only consumes relevant revenue data, keeping the pipeline efficient and organized.
# Because it represents a single, cohesive business event (a revenue transaction), it allows Kafka to partition the data efficiently. 
# In our producer code, we are providing messages by account_id
TOPIC = "revenue_transactions" 

# As your Spark job (streaming_job.py) processes incoming transactions every 30 seconds, it continuously logs its current position (offsets) in the Kafka topic and its internal progress to this folder.
# SIGNIFICANCE: If the Spark job crashes or is restarted, it can read this checkpoint data to resume processing exactly where it left off, ensuring no transactions are missed or double-counted.
# If Spark didn't save that state to a checkpoint, a system restart would completely wipe out its memory, forcing it to rebuild every account's financial baseline from scratch. This folder keeps that memory alive across application restarts.
# 3. Achieving "Exactly-Once" Guarantees
# By coupling the progress logs in streaming_checkpoints with a reliable storage target (like your Parquet files under streaming_output), Spark ensures Exactly-Once processing. 
# This prevents the system from accidentally double-counting transactions or writing duplicate anomaly alerts if a failure occurs mid-stream.
CHECKPOINT_ROOT = "streaming_checkpoints" # root directory where Spark saves its fault-tolerance and recovery metadata. 

"""
The Scenario
Imagine your Python producer sends a batch of 100 new revenue transactions into Kafka.
Your Spark streaming job picks up this batch to process it. Its job is to calculate the total revenue and append the results to a folder in your streaming_output/windowed_aggregates Parquet directory.

The Timeline of a Crash (Without Exactly-Once)
If a system doesn't have exactly-once safeguards, a mid-batch crash causes a mess:

Transactions 1 to 40: Spark processes them and writes the results out to the Parquet file.

Transaction 41: The power goes out! The server crashes instantly.

The Problem: Kafka thinks none of the 100 transactions were completed because the entire batch wasn't finished. When you turn Spark back on, it reads all 100 transactions again.

The Result: Transactions 1 through 40 get written to your Parquet files a second time, completely ruining your financial reporting with duplicate data.

The Solution: How Your Code Handles It
Because you configured CHECKPOINT_ROOT = "streaming_checkpoints" and are saving to a reliable target like Parquet, Spark prevents this duplicate nightmare using a two-step commit process:

1. The Mid-Stream Crash
When Spark starts processing those 100 transactions, it writes a "placeholder" or intent log into your streaming_checkpoints folder, stating exactly which data offsets it is currently working on.

If the crash happens at Transaction 41, Spark's checkpoint log remembers exactly where it broke down.

2. The Graceful Recovery
When you restart your streaming_job.py script, Spark doesn't guess where to start. It reads the checkpoint files first:

Step A (Clean up the target): Spark looks at your Parquet output directory. It identifies the unfinished file fragment from the crash (the data from transactions 1–40) and automatically discards or overwrites it so no partial, broken data remains.

Step B (Replay cleanly): Spark goes back to Kafka and asks to safely replay that specific batch of 100 transactions from the very beginning.

Because the broken data was wiped cleanly from the Parquet files before reprocessing began, the 100 transactions are written fresh.

Summary
To a business stakeholder looking at the final Parquet files, it looks like the crash never happened. Even though parts of the code executed twice behind the scenes, the end data result is Exactly-Once: no double-counted revenue, no duplicate anomaly alerts, and a perfectly accurate financial ledger.
"""


OUTPUT_ROOT = "streaming_output"

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("account_id", StringType()),
    StructField("source_system", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("event_time", StringType()),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("RevenueStreamReconciliation")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_transactions(spark: SparkSession):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", col("event_time").cast(TimestampType()))
    )
    return parsed


# ---------------------------------------------------------------------------
# 1) Windowed aggregation with watermarking
#    -> per-account, per-source-system revenue rollups every 2 minutes,
#       tolerating up to 3 minutes of late-arriving events before a window
#       is finalized. This is the "operational reporting" rollup.
# ---------------------------------------------------------------------------
def windowed_aggregates(parsed_df):
    return (
        parsed_df
        .withWatermark("event_time", "3 minutes")
        .groupBy(
            window(col("event_time"), "2 minutes"),
            col("account_id"),
            col("source_system"),
        )
        .agg(
            _sum("amount").alias("total_amount"),
            _count("transaction_id").alias("txn_count"),
        )
    )


# ---------------------------------------------------------------------------
# 2) Arbitrary stateful processing per account
#    -> maintains a running mean/count per account across the whole stream
#       (state persists across micro-batches, survives restarts via
#       checkpointing) and flags a transaction as anomalous if it deviates
#       sharply from that account's running baseline.
#       This is the "stateful processing" the JD asks for, distinct from
#       simple windowed aggregation.
# ---------------------------------------------------------------------------
state_schema = StructType([
    StructField("account_id", StringType()),
    StructField("running_count", DoubleType()),
    StructField("running_mean", DoubleType()),
])

output_schema = StructType([
    StructField("account_id", StringType()),
    StructField("transaction_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("running_mean_before", DoubleType()),
    StructField("is_anomaly", StringType()),
    StructField("detected_at", TimestampType()),
])


def detect_anomalies_per_account(key, pdf_iter, state):
    account_id = key[0]

    running_count = 0.0
    running_mean = 0.0
    if state.exists:
        stored_state = state.get
        if len(stored_state) == 3:
            _, running_count, running_mean = stored_state
        else:
            running_count, running_mean = stored_state

    results = []
    for pdf in pdf_iter:
        for _, row in pdf.iterrows():
            baseline_mean = running_mean if running_count > 0 else row["amount"]

            # flag if amount is >4x the account's running average (and we
            # have enough history to trust the baseline)
            is_anomaly = running_count >= 5 and abs(row["amount"]) > 4 * abs(baseline_mean)

            results.append({
                "account_id": account_id,
                "transaction_id": row["transaction_id"],
                "amount": row["amount"],
                "running_mean_before": baseline_mean,
                "is_anomaly": "true" if is_anomaly else "false",
                "detected_at": datetime.utcnow(),
            })

            # incremental mean update
            running_count += 1
            running_mean = running_mean + (row["amount"] - running_mean) / running_count

    state.update((account_id, running_count, running_mean))
    state.setTimeoutDuration(30 * 60 * 1000)  # 30 minutes, in milliseconds

    yield pd.DataFrame(results)


def stateful_anomaly_stream(parsed_df):
    return (
        parsed_df
        .withWatermark("event_time", "3 minutes")
        .groupBy("account_id")
        .applyInPandasWithState(
            detect_anomalies_per_account,
            outputStructType=output_schema,
            stateStructType=state_schema,
            outputMode="append",
            timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
        )
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    parsed = read_transactions(spark)

    # Sink 1: windowed revenue rollups -> console + parquet
    # Keep console logging for live debugging, and write the same
    # aggregate log to disk for later inspection.
    agg_query_console = (
        windowed_aggregates(parsed)
        .writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/windowed_agg_console")
        .trigger(processingTime="30 seconds")
        .start()
    )

    agg_query_file = (
        windowed_aggregates(parsed)
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", f"{OUTPUT_ROOT}/windowed_aggregates")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/windowed_agg_file")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # Sink 2: stateful anomaly flags -> parquet (stand-in for an
    # operational-reporting / alerting table)
    anomaly_query = (
        stateful_anomaly_stream(parsed)
        .filter(col("is_anomaly") == "true")
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", f"{OUTPUT_ROOT}/anomalies")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/anomalies")
        .trigger(processingTime="30 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
