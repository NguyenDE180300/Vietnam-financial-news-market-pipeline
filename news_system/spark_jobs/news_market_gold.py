"""Build a point-in-time News x Market event-window Gold dataset."""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, Window, functions as F, types as T


DEFAULT_OFFSETS = (-5, -3, -2, -1, 0, 1, 2, 3, 5)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--news-input", required=True)
    parser.add_argument("--market-input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--mode", choices=("overwrite", "append"), default="overwrite")
    parser.add_argument("--market-close-hour", type=int, default=15)
    parser.add_argument("--offsets", nargs="+", type=int, default=list(DEFAULT_OFFSETS))
    parser.add_argument("--output-partitions", type=int, default=4)
    args = parser.parse_args()
    if args.output_partitions < 1:
        parser.error("--output-partitions must be at least 1")

    spark = (
        SparkSession.builder.appName("vn-news-market-gold")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .config("spark.sql.shuffle.partitions", str(max(8, args.output_partitions * 2)))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        news = spark.read.parquet(args.news_input).filter("is_valid")
        market = (
            spark.read.parquet(args.market_input)
            .filter("is_valid")
            .select(
                "ticker", "session_date", "close", "daily_return", "source",
                "price_adjustment",
            )
        )

        # The market sequence is based on observed trading sessions, so T+n
        # skips weekends and holidays automatically.
        market_order = Window.partitionBy("ticker").orderBy("session_date")
        indexed_market = market.withColumn("session_index", F.row_number().over(market_order))

        # Equal-weight VN30 is deterministic and available from the same
        # source/session as each stock. It avoids mixing Yahoo stock prices
        # with a benchmark from a different vendor.
        benchmark = (
            market.groupBy("session_date")
            .agg(
                F.avg("daily_return").alias("benchmark_daily_return"),
                F.countDistinct("ticker").alias("benchmark_constituents"),
            )
        )
        benchmark_order = Window.orderBy("session_date").rowsBetween(
            Window.unboundedPreceding, Window.currentRow
        )
        benchmark = benchmark.withColumn(
            "benchmark_index",
            F.exp(F.sum(F.log1p(F.coalesce("benchmark_daily_return", F.lit(0.0)))).over(benchmark_order)),
        )

        events = (
            news
            .select(
                "event_id", "title", "source", "url", "published_precision",
                "published_timestamp", F.explode("merged_tickers").alias("ticker"),
            )
            .withColumn("event_at_vn", F.from_utc_timestamp("published_timestamp", "Asia/Ho_Chi_Minh"))
            .withColumn("event_date_vn", F.to_date("event_at_vn"))
            .withColumn("published_after_close", F.hour("event_at_vn") >= args.market_close_hour)
            .filter(F.col("event_at_vn").isNotNull())
        )

        e, m = events.alias("e"), indexed_market.alias("m")
        candidates = e.join(
            m,
            (F.col("e.ticker") == F.col("m.ticker"))
            & (
                (F.col("m.session_date") > F.col("e.event_date_vn"))
                | (
                    (F.col("m.session_date") == F.col("e.event_date_vn"))
                    & ~F.col("e.published_after_close")
                )
            ),
            "inner",
        )
        first_session = Window.partitionBy("e.event_id", "e.ticker").orderBy("m.session_date")
        anchors = (
            candidates
            .withColumn("_candidate_rank", F.row_number().over(first_session))
            .filter(F.col("_candidate_rank") == 1)
            .select(
                F.col("e.event_id"), F.col("e.ticker"), F.col("e.title"),
                F.col("e.source").alias("news_source"), F.col("e.url"),
                F.col("e.published_precision"), F.col("e.event_at_vn"),
                F.col("e.event_date_vn"), F.col("e.published_after_close"),
                F.col("m.session_date").alias("t0_date"),
                F.col("m.session_index").alias("t0_index"),
                F.col("m.source").alias("market_source"),
                F.col("m.price_adjustment"),
            )
        )

        offsets_schema = T.StructType([T.StructField("relative_session", T.IntegerType(), False)])
        offsets = spark.createDataFrame([(value,) for value in args.offsets], offsets_schema)
        requested = anchors.crossJoin(F.broadcast(offsets)).withColumn(
            "target_session_index", F.col("t0_index") + F.col("relative_session")
        )
        target = indexed_market.select(
            F.col("ticker").alias("target_ticker"),
            F.col("session_index").alias("target_index"),
            F.col("session_date"), F.col("close"), F.col("daily_return"),
        )
        window_rows = (
            requested.join(
                target,
                (F.col("ticker") == F.col("target_ticker"))
                & (F.col("target_session_index") == F.col("target_index")),
                "left",
            )
            .drop("target_ticker", "target_index")
        )

        anchor_stock = indexed_market.select(
            F.col("ticker").alias("anchor_ticker"),
            (F.col("session_index") + 1).alias("anchor_t0_index"),
            F.col("close").alias("anchor_close_t_minus_1"),
            F.col("session_date").alias("anchor_date_t_minus_1"),
        )
        enriched = window_rows.join(
            anchor_stock,
            (F.col("ticker") == F.col("anchor_ticker"))
            & (F.col("t0_index") == F.col("anchor_t0_index")),
            "left",
        ).drop("anchor_ticker", "anchor_t0_index")

        target_benchmark = benchmark.select(
            F.col("session_date").alias("benchmark_session_date"),
            "benchmark_daily_return", "benchmark_constituents", "benchmark_index",
        )
        anchor_benchmark = benchmark.select(
            F.col("session_date").alias("benchmark_anchor_date"),
            F.col("benchmark_index").alias("benchmark_anchor_index"),
        )
        gold = (
            enriched
            .join(target_benchmark, F.col("session_date") == F.col("benchmark_session_date"), "left")
            .join(
                anchor_benchmark,
                F.col("anchor_date_t_minus_1") == F.col("benchmark_anchor_date"),
                "left",
            )
            .withColumn("has_market_data", F.col("session_date").isNotNull())
            .withColumn(
                "stock_cumulative_return",
                F.when(
                    F.col("close").isNotNull() & F.col("anchor_close_t_minus_1").isNotNull(),
                    F.col("close") / F.col("anchor_close_t_minus_1") - 1.0,
                ),
            )
            .withColumn(
                "benchmark_cumulative_return",
                F.when(
                    F.col("benchmark_index").isNotNull() & F.col("benchmark_anchor_index").isNotNull(),
                    F.col("benchmark_index") / F.col("benchmark_anchor_index") - 1.0,
                ),
            )
            .withColumn(
                "abnormal_return",
                F.col("stock_cumulative_return") - F.col("benchmark_cumulative_return"),
            )
            .withColumn("gold_date", F.col("event_date_vn"))
            .drop("benchmark_session_date", "benchmark_anchor_date")
            .cache()
        )
        total = gold.count()
        events_count = gold.select("event_id", "ticker").distinct().count()
        available = gold.filter("has_market_data").count()
        gold.repartition(args.output_partitions, "gold_date").write.mode(args.mode).partitionBy(
            "gold_date"
        ).parquet(args.output)
        print(
            f"Gold rows={total} event_tickers={events_count} market_available={available} "
            f"output={args.output}"
        )
        gold.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
