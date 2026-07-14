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
import argparse
import shutil
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, concat, concat_ws, collect_list, lit, date_format, current_timestamp, sum as _sum, count as _count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.streaming.state import GroupStateTimeout

# ------ Beginner / non-technical primer ------
# This file implements a small, local streaming pipeline using Kafka and Spark.
# Think of it like a conveyor belt where small 'transaction' cards flow in from
# Kafka (the producer puts them there), and Spark inspects each card to:
#  1) periodically summarize revenue by account (windowed aggregation)
#  2) keep a running memory per account and flag unusually large transactions
# The comments marked **Interview** highlight points that are often asked in
# interviews for streaming / data-engineer roles.

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
CHECKPOINT_ROOT = "streaming_checkpoints"  # root directory where Spark saves its fault-tolerance and recovery metadata.
CHECKPOINT_VERSION = "v2"  # bump this when the query or state schema changes to avoid incompatible checkpoint reuse.
ENABLE_CONSOLE_DEBUG = False  # production best practice: avoid console sinks, use durable storage and monitoring instead.

# Optional runtime flag to force a fresh restart without checkpoint recovery.
# Use it when schema/state changes make the existing checkpoint invalid.
FORCE_FRESH_START = False


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the streaming revenue reconciliation job."
    )
    parser.add_argument(
        "--checkpoint-version",
        default=CHECKPOINT_VERSION,
        help="checkpoint version to use; bump this when query or state schema changes",
    )
    parser.add_argument(
        "--force-fresh-start",
        action="store_true",
        help="delete the checkpoint folder for the active version and clear local Parquet output before starting",
    )
    parser.add_argument(
        "--clear-output",
        action="store_true",
        help="delete the local Parquet output folders before starting",
    )
    parser.add_argument(
        "--enable-console-debug",
        action="store_true",
        help="enable the local console debug sink for windowed aggregates",
    )
    return parser.parse_args()


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

# Explanation of the schema fields (for beginners):
# - `transaction_id`: unique id for the money movement
# - `account_id`: business account this transaction belongs to (used as the key)
# - `source_system`: which system reported the transaction (billing, POS, etc.)
# - `amount`: numeric value of the transaction
# - `currency`: currency code (e.g., USD)
# - `event_time`: the event timestamp as an ISO string; later cast to Spark Timestamp

# Interview: Why is a stable schema important in streaming systems?
# - Changing the schema (adding/removing fields or changing types) can break
#   checkpoint compatibility and cause state deserialization errors. Best practice
#   is to version events or handle backward compatibility in code.

# initialization engine for your entire processing job. It sets up the environment that allows Python to coordinate massive, parallel data workloads.
# .config("spark.sql.shuffle.partitions", "4") : This is a crucial performance tuning configuration, and it is highly significant for your local setup.
# Whenever Spark does grouping or aggregation operations (like your groupBy in windowed_aggregates or stateful_anomaly_stream), it has to rearrange, sort, and move data across its internal memory streams. This data movement process is known as a Shuffle.
# The Problem: By default, Spark splits shuffled data into 200 separate partitions (smaller work buckets). While 200 is great for a massive cloud data center with dozens of machines, running 200 concurrent tasks on your personal computer will completely overload it, causing the script to run painfully slow or crash due to memory overhead.
# The Solution: By explicitly overriding this configuration to "4", you are limiting Spark to use exactly 4 data buckets during a shuffle. This matches the smaller scale of a local laptop or a simple Docker container setup, ensuring the streaming pipeline processes batches lightning-fast without choking your computer's CPU.
def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("RevenueStreamReconciliation") # In a production setting, a company might run hundreds of data pipelines simultaneously on a shared cluster. Giving your job a clear business name allows engineers to easily locate it in the live Spark Web UI dashboard to track performance, monitor memory usage, or debug errors.
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate() # This is the standard entry point for any modern Apache Spark application. Instead of spinning up a fresh, heavy background environment every time you run a command, getOrCreate() checks if an active Spark environment is already running in your system memory. If it exists, it safely hooks into it; if not, it creates a brand new one.
    )


def read_transactions(spark: SparkSession):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", False)
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", col("event_time").cast(TimestampType()))
    )
    return parsed

# Notes for beginners:
# - `failOnDataLoss=false` means Spark will continue when Kafka log offsets move
#   backwards because old data has been deleted or compacted. It treats that as
#   acceptable data loss instead of failing the query.
# - In local Kafka setups, the broker may expire older messages quickly, which is
#   why this option is useful for development but not always safe for production.
#
# Interview: What should you do if you see an offset jump like this?
# - Inspect Kafka retention and topic cleanup settings.
# - If you need guaranteed replay, make sure the broker retains all required
#   offsets long enough or use an external durable storage like a changelog.
# - For a demo/dev environment, `failOnDataLoss=false` is acceptable to keep the
#   stream running, but it means some records could be skipped.
# - `startingOffsets="latest"` means when this job starts it will only read
#   new messages produced after the job starts. For development you might use
#   "earliest" to reprocess existing test data.
# - Kafka messages include a key and a value. In our producer we set the key to
#   `account_id` so records for the same account are likely to be read in the
#   same partition and processed together (helps with stateful ops).
#
# Interview: What does partitioning buy you in Kafka + Spark?
# - Partitioning enables parallelism: multiple Spark tasks can read different
#   partitions concurrently. Keys map to partitions so related messages stay
#   ordered within a partition.


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
        .withColumn(
            "aggregation_id",
            concat(
                col("account_id"),
                lit("_"),
                col("source_system"),
                lit("_"),
                date_format(col("window").start, "yyyyMMddHHmmss"),
            ),
        )
        .withColumn("batch_timestamp", current_timestamp())
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


def print_batch_with_timestamp(df, epoch_id):
    batch_time = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    print("=" * 90)
    print(f"Batch: {epoch_id}    timestamp: {batch_time}")
    print("=" * 90)
    df.show(truncate=False)
    print("\n")


def clean_checkpoint_dir(checkpoint_version: str):
    checkpoint_path = (Path(CHECKPOINT_ROOT) / checkpoint_version).resolve()
    if checkpoint_path.exists():
        print(f"Removing corrupted checkpoint directory: {checkpoint_path}")
        shutil.rmtree(checkpoint_path)
    else:
        print(f"No checkpoint directory to remove at: {checkpoint_path}")


def clean_output_dirs():
    for subdir in ["windowed_aggregates", "anomalies"]:
        output_path = (Path(OUTPUT_ROOT) / subdir).resolve()
        if output_path.exists():
            print(f"Removing stale output directory: {output_path}")
            shutil.rmtree(output_path)
        else:
            print(f"No stale output directory to remove at: {output_path}")


def main():
    args = parse_args()
    checkpoint_version = args.checkpoint_version
    enable_console_debug = args.enable_console_debug
    force_fresh_start = args.force_fresh_start
    clear_output = args.clear_output

    if force_fresh_start:
        clean_checkpoint_dir(checkpoint_version)
        clean_output_dirs()
    elif clear_output:
        clean_output_dirs()

    print(f"Starting Spark with checkpoint version: {checkpoint_version}")
    print(f"Fresh start: {force_fresh_start}, clear output: {clear_output}")

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")  # This way we see less message noise in the console, focusing on the key outputs and errors.

    parsed = read_transactions(spark)

    # Sink 1: windowed revenue rollups -> parquet
    # Production best practice is to write aggregate output to durable storage
    # instead of relying on console output. Console output is only useful for
    # local debugging, and should be disabled on a real deployment.
    windowed_df = windowed_aggregates(parsed)

    if enable_console_debug:
        # Use the built-in console sink for local debugging. This avoids the
        # Python foreachBatch callback path and its separate checkpoint state
        # store, which can become inconsistent in local Windows/SQLite-like
        # recovery scenarios.
        _ = (
            windowed_df
            .writeStream
            .format("console")
            .outputMode("update")
            .option("truncate", False)
            .option("numRows", 100)
            .trigger(processingTime="30 seconds")
            .start()
        )

    agg_query_file = (
        windowed_df
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", f"{OUTPUT_ROOT}/windowed_aggregates")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{checkpoint_version}/windowed_agg_file")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # Notes about sinks and file outputs (beginner + interview notes):
    # - We write windowed rollups to Parquet in `append` mode. Parquet sinks
    #   create internal metadata under a `_spark_metadata` folder. If that
    #   metadata becomes inconsistent (for example after a schema or state
    #   change), Spark can raise errors like `BATCH_METADATA_NOT_FOUND`.
    #   Recovery often requires deleting the affected `streaming_output/...`
    #   folder and the corresponding `streaming_checkpoints/...` folder to
    #   allow a fresh run.
    # - `trigger(processingTime="30 seconds")` means Spark will attempt to
    #   process new data every 30 seconds. This is a simple micro-batch model.
    #
    # Interview: What is the difference between `append` and `update` modes?
    # - `append`: only new rows are written to the sink (good for file sinks).
    # - `update`: writes changed rows (requires a sink that supports updates).
    # File-based sinks like Parquet typically require `append`.

    # Sink 2: stateful anomaly flags -> parquet (stand-in for an
    # operational-reporting / alerting table)
    anomaly_query = (
        stateful_anomaly_stream(parsed)
        .filter(col("is_anomaly") == "true")
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", f"{OUTPUT_ROOT}/anomalies")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{checkpoint_version}/anomalies")
        .trigger(processingTime="30 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
