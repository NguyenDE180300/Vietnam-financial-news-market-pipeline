"""Consume Kafka news events with Spark and write micro-batches to ADLS Bronze."""
from __future__ import annotations

import argparse
import gzip
import sys
from datetime import datetime, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession, functions as F

from news_system.lake_storage import create_lake_storage
from news_system.spark_jobs.bronze_to_silver import NEWS_SCHEMA


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="news.raw.v1")
    parser.add_argument("--account-name", required=True)
    parser.add_argument("--checkpoint", default="data_lake/checkpoints/news_kafka_to_bronze")
    parser.add_argument("--starting-offsets", choices=("earliest", "latest"), default="earliest")
    parser.add_argument("--trigger", choices=("available-now", "continuous"), default="continuous")
    parser.add_argument("--interval", default="30 seconds")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("vn-news-kafka-to-adls-bronze")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    storage = create_lake_storage("adls", account_name=args.account_name)

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )
    stream = (
        kafka_stream
        .withColumn("raw_payload", F.col("value").cast("string"))
        .withColumn("news", F.from_json("raw_payload", NEWS_SCHEMA))
        .filter(F.col("news").isNotNull())
        .select(
            "news.*",
            "raw_payload",
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").cast("string").alias("kafka_timestamp"),
        )
    )

    def write_batch(dataframe, batch_id: int) -> None:
        rows = dataframe.toJSON().collect()
        if not rows:
            return
        now = datetime.now(timezone.utc)
        payload = gzip.compress(("\n".join(rows) + "\n").encode("utf-8"), compresslevel=6)
        path = (
            f"news_stream/ingest_date={now:%Y-%m-%d}/hour={now:%H}/"
            f"spark_batch_{batch_id:020d}.jsonl.gz"
        )
        target = storage.write_bytes("bronze", path, payload)
        print(f"Spark Bronze batch={batch_id} records={len(rows)} path={target}", flush=True)

    writer = (
        stream.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", args.checkpoint)
        .queryName("vn_news_kafka_to_adls_bronze")
    )
    query = (
        writer.trigger(availableNow=True).start()
        if args.trigger == "available-now"
        else writer.trigger(processingTime=args.interval).start()
    )
    try:
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
