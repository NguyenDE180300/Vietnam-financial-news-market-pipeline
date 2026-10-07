"""Validate the contract and data quality of a Silver news Parquet dataset."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession, functions as F

from news_system.ticker_extractor import VN30_TICKERS


REQUIRED_COLUMNS = {
    "event_id",
    "source",
    "url",
    "title",
    "published_timestamp",
    "collected_timestamp",
    "published_date",
    "text_for_extraction",
    "content_hash",
    "rulebased_tickers",
    "ner_tickers",
    "merged_tickers",
    "data_quality_errors",
    "is_valid",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Silver Parquet path")
    parser.add_argument("--expected-min-rows", type=int, default=1)
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("vn-news-validate-silver")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        news = spark.read.parquet(args.input).cache()
        missing_columns = sorted(REQUIRED_COLUMNS - set(news.columns))
        if missing_columns:
            raise SystemExit(f"FAILED missing_columns={','.join(missing_columns)}")

        total = news.count()
        distinct_events = news.select("event_id").distinct().count()
        invalid = news.filter(~F.col("is_valid")).count()
        empty_titles = news.filter(F.length(F.trim("title")) == 0).count()
        outside_vn30 = (
            news.select(F.explode_outer("merged_tickers").alias("ticker"))
            .filter(F.col("ticker").isNotNull() & ~F.col("ticker").isin(sorted(VN30_TICKERS)))
            .count()
        )
        with_ticker = news.filter(F.size("merged_tickers") > 0).count()
        with_full_text = news.filter("has_full_text").count()

        print(
            "Silver validation: "
            f"rows={total} distinct_event_id={distinct_events} invalid={invalid} "
            f"empty_title={empty_titles} outside_vn30={outside_vn30} "
            f"full_text={with_full_text} with_vn30_ticker={with_ticker}"
        )

        failures = []
        if total < args.expected_min_rows:
            failures.append(f"rows<{args.expected_min_rows}")
        if distinct_events != total:
            failures.append(f"duplicate_event_id={total - distinct_events}")
        if invalid:
            failures.append(f"invalid_rows={invalid}")
        if empty_titles:
            failures.append(f"empty_titles={empty_titles}")
        if outside_vn30:
            failures.append(f"tickers_outside_vn30={outside_vn30}")
        if failures:
            raise SystemExit("FAILED " + " ".join(failures))

        print("PASSED Silver quality gate")
        news.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
