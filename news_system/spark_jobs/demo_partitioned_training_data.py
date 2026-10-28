"""Demonstrate Spark partitioned processing on the existing Gold snapshot.

This is a local, single-machine parallelism demo, not a multi-node Spark cluster.
It only reads the source Gold and writes to a separate demonstration directory.
"""
from __future__ import annotations

import argparse
import json

from pyspark.sql import SparkSession, functions as F
from pyspark.storagelevel import StorageLevel


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", default="data_lake/gold/training_targets_fulltext_attention_v1",
    )
    parser.add_argument(
        "--output", default="data_lake/demo/spark_partitioned_training_targets_v1",
    )
    parser.add_argument("--partitions", type=int, default=4)
    args = parser.parse_args()
    if args.partitions < 1:
        parser.error("--partitions must be positive")

    spark = (
        SparkSession.builder.appName("demo-partitioned-training-data")
        .config("spark.sql.shuffle.partitions", str(args.partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        source = spark.read.parquet(args.input)
        required = {"event_id", "ticker", "training_year", "mask_t1", "mask_t3", "mask_t5"}
        missing = sorted(required - set(source.columns))
        if missing:
            raise SystemExit("FAILED missing_columns=" + ",".join(missing))

        partitioned = source.repartition(args.partitions).persist(StorageLevel.MEMORY_AND_DISK)
        total = partitioned.count()
        per_partition = (
            partitioned
            .withColumn("_spark_partition_id", F.spark_partition_id())
            .groupBy("_spark_partition_id")
            .agg(
                F.count("*").alias("rows"),
                F.countDistinct("event_id").alias("articles"),
                F.countDistinct("ticker").alias("tickers"),
                F.sum(F.col("mask_t1").cast("int")).alias("usable_t1_targets"),
                F.sum(F.col("mask_t3").cast("int")).alias("usable_t3_targets"),
                F.sum(F.col("mask_t5").cast("int")).alias("usable_t5_targets"),
            )
            .orderBy("_spark_partition_id")
            .collect()
        )

        # Persist a separate physical Parquet copy to make the partitioned
        # output inspectable. Never overwrite the source Gold path.
        partitioned.write.mode("overwrite").parquet(args.output)
        result = {
            "execution_mode": spark.sparkContext.master,
            "default_parallelism": spark.sparkContext.defaultParallelism,
            "requested_partitions": args.partitions,
            "actual_partitions": partitioned.rdd.getNumPartitions(),
            "source_rows": total,
            "partition_rows": [row.asDict() for row in per_partition],
            "partition_row_sum": sum(row["rows"] for row in per_partition),
            "output_path": args.output,
            "cluster_note": "Local single-machine parallelism; no multi-node executors.",
        }
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        if result["partition_row_sum"] != total:
            raise SystemExit("FAILED partition row counts do not match source row count")
        if result["actual_partitions"] != args.partitions:
            raise SystemExit("FAILED actual partition count differs from requested count")
        print("PASSED partitioned Spark demo")
        partitioned.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
