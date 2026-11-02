"""
Spark Structured Streaming Job:  Kafka  ->  Spark  ->  MongoDB
==============================================================

Reads the weather readings that producer.py publishes to Kafka and runs two
continuous streaming queries:

1. CLEAN & STORE (weather_raw)
   - parses the JSON messages against a fixed schema
   - drops malformed records (missing ids / values / timestamps)
   - converts timestamps and adds a data-quality flag (value inside the
     physically plausible range for its metric?)
   - upserts every reading into MongoDB (idempotent: replaying Kafka never
     creates duplicates)

2. WINDOWED AGGREGATION (weather_agg_windowed)
   - groups readings into tumbling event-time windows (default 5 minutes)
     per city and metric, with a watermark for late data
   - computes avg / min / max / stddev / count
   - upserts each window into MongoDB as it is updated

Each micro-batch also writes a heartbeat to `pipeline_status`, which the
dashboard uses to show whether Spark is running.

Run locally (needs Java 17 and `pip install pyspark==3.5.3`):
    python spark_streaming.py
Run in Docker:
    docker compose up spark
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField, StructType)

import config

MESSAGE_SCHEMA = StructType([
    StructField("timestamp", StringType()),
    StructField("value", DoubleType()),
    StructField("metric_type", StringType()),
    StructField("sensor_id", StringType()),
    StructField("location", StringType()),
    StructField("unit", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("observed_at", StringType()),
    StructField("weather_code", IntegerType()),
    StructField("weather_desc", StringType()),
    StructField("source", StringType()),
])

ISO_FMT = "yyyy-MM-dd'T'HH:mm:ss'Z'"


def build_spark() -> SparkSession:
    import pyspark
    major = int(pyspark.__version__.split(".")[0])
    scala = "2.13" if major >= 4 else "2.12"
    kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{pyspark.__version__}"
    return (
        SparkSession.builder
        .appName("WeatherStreamProcessor")
        # Ignored when launched with spark-submit --packages (Docker); used for `python spark_streaming.py`
        .config("spark.jars.packages", kafka_pkg)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )


def _parse_iso(s):
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


# ---------------------------------------------------------------------------
# MongoDB writers (run on Spark executors, one client per partition)
# ---------------------------------------------------------------------------
def _write_raw_partition(rows):
    from pymongo import MongoClient, ReplaceOne
    ops = []
    now = datetime.now(timezone.utc)
    for r in rows:
        doc = r.asDict()
        doc["timestamp"] = _parse_iso(doc.pop("ts_iso"))
        doc["ingested_at"] = now
        doc["_id"] = f"{doc['sensor_id']}|{doc['metric_type']}|{doc['timestamp'].isoformat()}"
        ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
    if ops:
        client = MongoClient(config.MONGO_URI)
        try:
            client[config.MONGO_DB][config.RAW_COLLECTION].bulk_write(ops, ordered=False)
        finally:
            client.close()


def _write_agg_partition(rows):
    from pymongo import MongoClient, ReplaceOne
    ops = []
    now = datetime.now(timezone.utc)
    for r in rows:
        doc = r.asDict()
        doc["window_start"] = _parse_iso(doc["window_start"])
        doc["window_end"] = _parse_iso(doc["window_end"])
        doc["updated_at"] = now
        doc["_id"] = f"{doc['sensor_id']}|{doc['metric_type']}|{doc['window_start'].isoformat()}"
        ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
    if ops:
        client = MongoClient(config.MONGO_URI)
        try:
            client[config.MONGO_DB][config.AGG_COLLECTION].bulk_write(ops, ordered=False)
        finally:
            client.close()


def _heartbeat(query_name: str, batch_id: int, rows: int):
    from pymongo import MongoClient
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        client[config.MONGO_DB][config.STATUS_COLLECTION].update_one(
            {"_id": query_name},
            {"$set": {"last_batch_at": datetime.now(timezone.utc), "last_batch_id": batch_id,
                      "last_batch_rows": rows},
             "$inc": {"total_rows": rows}},
            upsert=True,
        )
    finally:
        client.close()


def write_raw_batch(batch_df: DataFrame, batch_id: int):
    batch_df = batch_df.persist()
    try:
        n = batch_df.count()
        if n:
            batch_df.foreachPartition(_write_raw_partition)
        _heartbeat("spark_raw", batch_id, n)
        if n:
            print(f"[raw] batch {batch_id}: stored {n} readings", flush=True)
    finally:
        batch_df.unpersist()


def write_agg_batch(batch_df: DataFrame, batch_id: int):
    batch_df = batch_df.persist()
    try:
        n = batch_df.count()
        if n:
            batch_df.foreachPartition(_write_agg_partition)
        _heartbeat("spark_agg", batch_id, n)
        if n:
            print(f"[agg] batch {batch_id}: updated {n} windows", flush=True)
    finally:
        batch_df.unpersist()


# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------
def quality_flag_expr():
    """'ok' if value is inside the plausible range for its metric, else 'out_of_range'."""
    expr = None
    for meta in config.METRICS.values():
        lo, hi = meta["valid"]
        cond = (F.col("metric_type") == meta["metric_type"]) & F.col("value").between(lo, hi)
        expr = cond if expr is None else (expr | cond)
    return F.when(expr, F.lit("ok")).otherwise(F.lit("out_of_range"))


def parse_and_clean(kafka_df: DataFrame) -> DataFrame:
    parsed = (
        kafka_df
        .select(
            F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("d"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
        .select("d.*", "kafka_partition", "kafka_offset")
    )
    return (
        parsed
        .filter(F.col("sensor_id").isNotNull() & F.col("metric_type").isNotNull()
                & F.col("value").isNotNull() & F.col("timestamp").isNotNull())
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .filter(F.col("event_time").isNotNull())
        .withColumn("value", F.round("value", 2))
        .withColumn("quality", quality_flag_expr())
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    # ship config.py to the executors (the MongoDB writers import it)
    spark.sparkContext.addPyFile(os.path.abspath(config.__file__))
    ckpt = os.path.abspath(config.SPARK_CHECKPOINT_DIR)

    print("=" * 70)
    print(f"Spark {spark.version} | Kafka {config.KAFKA_BOOTSTRAP_SERVERS} topic '{config.KAFKA_TOPIC}'")
    print(f"MongoDB {config.MONGO_URI} db '{config.MONGO_DB}' | window {config.SPARK_AGG_WINDOW}")
    print("=" * 70, flush=True)

    _ensure_indexes()

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    clean = parse_and_clean(kafka_df)

    # Query 1: cleaned readings -> weather_raw
    raw_out = clean.select(
        F.date_format("event_time", ISO_FMT).alias("ts_iso"),
        "value", "metric_type", "sensor_id", "location", "unit", "latitude", "longitude",
        "observed_at", "weather_code", "weather_desc", "source", "quality",
        "kafka_partition", "kafka_offset",
    )
    raw_query = (
        raw_out.writeStream.queryName("raw_to_mongo")
        .foreachBatch(write_raw_batch)
        .option("checkpointLocation", os.path.join(ckpt, "raw"))
        .trigger(processingTime="10 seconds")
        .start()
    )

    # Query 2: windowed aggregation -> weather_agg_windowed
    agg = (
        clean.filter(F.col("quality") == "ok")
        .withWatermark("event_time", config.SPARK_WATERMARK)
        .groupBy(F.window("event_time", config.SPARK_AGG_WINDOW),
                 "sensor_id", "location", "metric_type", "unit")
        .agg(F.round(F.avg("value"), 2).alias("avg"),
             F.min("value").alias("min"),
             F.max("value").alias("max"),
             F.round(F.stddev_pop("value"), 3).alias("stddev"),
             F.count("*").alias("count"))
        .select(F.date_format("window.start", ISO_FMT).alias("window_start"),
                F.date_format("window.end", ISO_FMT).alias("window_end"),
                "sensor_id", "location", "metric_type", "unit", "avg", "min", "max", "stddev", "count")
    )
    agg_query = (
        agg.writeStream.queryName("windowed_agg_to_mongo")
        .outputMode("update")
        .foreachBatch(write_agg_batch)
        .option("checkpointLocation", os.path.join(ckpt, "agg"))
        .trigger(processingTime="15 seconds")
        .start()
    )

    print("Streaming queries started. Press Ctrl+C to stop.", flush=True)
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("Stopping streaming queries...")
    finally:
        for q in (raw_query, agg_query):
            try:
                q.stop()
            except Exception:
                pass
        spark.stop()


def _ensure_indexes():
    """Create the indexes the dashboard queries rely on (safe to run repeatedly)."""
    from pymongo import ASCENDING, DESCENDING, MongoClient
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=10000)
    try:
        db = client[config.MONGO_DB]
        db[config.RAW_COLLECTION].create_index([("timestamp", DESCENDING)])
        db[config.RAW_COLLECTION].create_index([("location", ASCENDING), ("metric_type", ASCENDING),
                                                ("timestamp", DESCENDING)])
        db[config.AGG_COLLECTION].create_index([("window_start", DESCENDING)])
        db[config.AGG_COLLECTION].create_index([("location", ASCENDING), ("metric_type", ASCENDING),
                                                ("window_start", DESCENDING)])
    finally:
        client.close()


if __name__ == "__main__":
    main()
