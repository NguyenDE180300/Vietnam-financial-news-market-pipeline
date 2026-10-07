"""Quality gate for the Silver daily-market dataset."""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F


REQUIRED = {
    "event_id", "ticker", "session_date", "open", "high", "low", "close",
    "volume", "source", "daily_return", "log_return", "data_quality_errors", "is_valid",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--expected-min-rows", type=int, default=1)
    parser.add_argument("--expected-min-tickers", type=int, default=1)
    args = parser.parse_args()
    spark = (
        SparkSession.builder.appName("vn-market-validate-silver")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        market = spark.read.parquet(args.input).cache()
        missing = sorted(REQUIRED - set(market.columns))
        if missing:
            raise SystemExit("FAILED missing_columns=" + ",".join(missing))
        total = market.count()
        tickers = market.select("ticker").distinct().count()
        distinct_keys = market.select("ticker", "session_date", "interval", "source").distinct().count()
        invalid = market.filter(~F.col("is_valid")).count()
        weekend = market.filter(F.dayofweek("session_date").isin(1, 7)).count()
        extreme_returns = market.filter(F.abs("daily_return") > 0.5).count()
        print(
            f"Market validation: rows={total} tickers={tickers} distinct_keys={distinct_keys} "
            f"invalid={invalid} weekend={weekend} abs_return_gt_50pct={extreme_returns}"
        )
        failures = []
        if total < args.expected_min_rows:
            failures.append(f"rows<{args.expected_min_rows}")
        if tickers < args.expected_min_tickers:
            failures.append(f"tickers<{args.expected_min_tickers}")
        if distinct_keys != total:
            failures.append(f"duplicate_keys={total - distinct_keys}")
        if invalid:
            failures.append(f"invalid_rows={invalid}")
        if weekend:
            failures.append(f"weekend_rows={weekend}")
        if extreme_returns:
            failures.append(f"extreme_returns={extreme_returns}")
        if failures:
            raise SystemExit("FAILED " + " ".join(failures))
        print("PASSED Market Silver quality gate")
        market.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
