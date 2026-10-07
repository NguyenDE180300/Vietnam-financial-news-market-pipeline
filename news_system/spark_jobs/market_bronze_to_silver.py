"""Spark job: Bronze daily market bars to validated Silver Parquet."""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, Window, functions as F, types as T


MARKET_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("schema_version", T.IntegerType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("ticker", T.StringType()),
    T.StructField("session_date", T.StringType()),
    T.StructField("interval", T.StringType()),
    T.StructField("open", T.DoubleType()),
    T.StructField("high", T.DoubleType()),
    T.StructField("low", T.DoubleType()),
    T.StructField("close", T.DoubleType()),
    T.StructField("volume", T.DoubleType()),
    T.StructField("currency", T.StringType()),
    T.StructField("price_adjustment", T.StringType()),
    T.StructField("source", T.StringType()),
    T.StructField("collected_at_utc", T.StringType()),
    T.StructField("ingest_date", T.StringType()),
])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--mode", choices=("overwrite", "append"), default="overwrite")
    parser.add_argument("--output-partitions", type=int, default=4)
    args = parser.parse_args()
    if args.output_partitions < 1:
        parser.error("--output-partitions must be at least 1")

    spark = (
        SparkSession.builder.appName("vn-market-bronze-to-silver")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .config("spark.sql.shuffle.partitions", str(max(8, args.output_partitions * 2)))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        bronze = spark.read.schema(MARKET_SCHEMA).option("recursiveFileLookup", "true").json(args.input)
        latest = (
            bronze
            .withColumn("ticker", F.upper(F.trim("ticker")))
            .withColumn("source", F.lower(F.trim("source")))
            .withColumn("session_date", F.to_date("session_date"))
            .withColumn("collected_timestamp", F.to_timestamp("collected_at_utc"))
            .withColumn(
                "_rank",
                F.row_number().over(
                    Window.partitionBy("ticker", "session_date", "interval", "source")
                    .orderBy(F.col("collected_timestamp").desc_nulls_last())
                ),
            )
            .filter(F.col("_rank") == 1)
            .drop("_rank")
        )
        quality_errors = F.array_compact(F.array(
            F.when(F.col("event_id").isNull(), F.lit("missing_event_id")),
            F.when(~F.col("ticker").rlike("^[A-Z0-9]{2,10}$"), F.lit("invalid_ticker")),
            F.when(F.col("session_date").isNull(), F.lit("invalid_session_date")),
            F.when(F.col("interval") != "1d", F.lit("unsupported_interval")),
            F.when(F.col("open").isNull() | (F.col("open") <= 0), F.lit("invalid_open")),
            F.when(F.col("high").isNull() | (F.col("high") <= 0), F.lit("invalid_high")),
            F.when(F.col("low").isNull() | (F.col("low") <= 0), F.lit("invalid_low")),
            F.when(F.col("close").isNull() | (F.col("close") <= 0), F.lit("invalid_close")),
            F.when(F.col("high") < F.greatest("open", "close", "low"), F.lit("high_below_ohlc")),
            F.when(F.col("low") > F.least("open", "close", "high"), F.lit("low_above_ohlc")),
            F.when(F.col("volume").isNull() | (F.col("volume") < 0), F.lit("invalid_volume")),
            F.when(F.col("collected_timestamp").isNull(), F.lit("invalid_collected_at")),
            F.when(~F.col("schema_version").eqNullSafe(F.lit(1)), F.lit("unsupported_schema")),
        ))
        ordering = Window.partitionBy("ticker", "source").orderBy("session_date")
        silver = (
            latest
            .withColumn("previous_close", F.lag("close").over(ordering))
            .withColumn("daily_return", F.col("close") / F.col("previous_close") - F.lit(1.0))
            .withColumn("log_return", F.log(F.col("close") / F.col("previous_close")))
            .withColumn("data_quality_errors", quality_errors)
            .withColumn("is_valid", F.size("data_quality_errors") == 0)
            .cache()
        )
        total = silver.count()
        valid = silver.filter("is_valid").count()
        tickers = silver.select("ticker").distinct().count()
        silver.repartition(args.output_partitions, "session_date").write.mode(args.mode).partitionBy(
            "session_date"
        ).parquet(args.output)
        print(f"Market Silver rows={total} valid={valid} tickers={tickers} output={args.output}")
        silver.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
