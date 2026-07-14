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
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        streaming_job.py
"""
from datetime import datetime

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as _sum, count as _count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.streaming.state import GroupStateTimeout

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "revenue_transactions"
CHECKPOINT_ROOT = "streaming_checkpoints"
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
