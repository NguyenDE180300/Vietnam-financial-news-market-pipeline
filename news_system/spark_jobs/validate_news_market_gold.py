"""Quality gate for point-in-time News x Market Gold rows."""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F


EXPECTED_OFFSETS = {-5, -3, -2, -1, 0, 1, 2, 3, 5}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--expected-min-event-tickers", type=int, default=1)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("validate-news-market-gold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        gold = spark.read.parquet(args.input).cache()
        total = gold.count()
        event_tickers = gold.select("event_id", "ticker").distinct().count()
        distinct_keys = gold.select("event_id", "ticker", "relative_session").distinct().count()
        offsets = {row[0] for row in gold.select("relative_session").distinct().collect()}
        bad_t0 = gold.filter(
            (F.col("published_after_close") & (F.col("t0_date") <= F.col("event_date_vn")))
            | (~F.col("published_after_close") & (F.col("t0_date") < F.col("event_date_vn")))
        ).count()
        bad_missing = gold.filter(~F.col("has_market_data") & F.col("close").isNotNull()).count()
        available = gold.filter("has_market_data").count()
        print(
            f"Gold validation: rows={total} event_tickers={event_tickers} "
            f"distinct_keys={distinct_keys} available={available} bad_t0={bad_t0} "
            f"bad_missing_flag={bad_missing} offsets={sorted(offsets)}"
        )
        failures = []
        if event_tickers < args.expected_min_event_tickers:
            failures.append(f"event_tickers<{args.expected_min_event_tickers}")
        if total != distinct_keys:
            failures.append(f"duplicate_keys={total - distinct_keys}")
        if offsets != EXPECTED_OFFSETS:
            failures.append("unexpected_offsets")
        if total != event_tickers * len(EXPECTED_OFFSETS):
            failures.append("incomplete_event_windows")
        if bad_t0:
            failures.append(f"lookahead_t0={bad_t0}")
        if bad_missing:
            failures.append(f"bad_missing_flag={bad_missing}")
        if failures:
            raise SystemExit("FAILED " + " ".join(failures))
        print("PASSED News-Market Gold quality gate")
        gold.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
