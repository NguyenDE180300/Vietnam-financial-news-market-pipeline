"""Spark batch job: Bronze news JSONL(.gz) to deduplicated Silver Parquet."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession, Window, functions as F, types as T

from news_system.ticker_extractor import TickerExtractor, VN30_TICKERS


NEWS_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType(), False),
    T.StructField("schema_version", T.IntegerType(), False),
    T.StructField("event_type", T.StringType()),
    T.StructField("source", T.StringType()),
    T.StructField("url", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("summary", T.StringType()),
    T.StructField("content", T.StringType()),
    T.StructField("author", T.StringType()),
    T.StructField("image_url", T.StringType()),
    T.StructField("published_at_raw", T.StringType()),
    T.StructField("published_at_utc", T.StringType()),
    T.StructField("published_at_vn", T.StringType()),
    T.StructField("published_precision", T.StringType()),
    T.StructField("collected_at_utc", T.StringType()),
    T.StructField("ingest_date", T.StringType()),
])

_TICKER_EXTRACTOR = TickerExtractor()
_VN30 = set(VN30_TICKERS)


def _extract_vn30_tickers(text: str | None) -> list[str]:
    return [ticker for ticker in _TICKER_EXTRACTOR.extract(text or "") if ticker in _VN30]


def _build_ner_udf(model_path: str):
    # Each Python worker loads the model once on first use, not once per row.
    holder = {"extractor": None}

    def extract(text: str | None) -> list[str]:
        if holder["extractor"] is None:
            from news_system.ner_ticker import CRFTickerNER
            holder["extractor"] = CRFTickerNER(model_path)
        return [ticker for ticker in holder["extractor"].extract(text or "") if ticker in _VN30]

    return F.udf(extract, T.ArrayType(T.StringType(), containsNull=False))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Bronze path, local or configured abfss://")
    parser.add_argument("--output", required=True, help="Silver Parquet path")
    parser.add_argument("--mode", choices=("overwrite", "append"), default="overwrite")
    parser.add_argument("--ner-model", help="Optional local CRF model for parallel NER extraction")
    parser.add_argument(
        "--output-partitions",
        type=int,
        default=4,
        help="Number of output tasks/files before date partitioning (default: 4)",
    )
    args = parser.parse_args()

    if args.output_partitions < 1:
        parser.error("--output-partitions must be at least 1")

    spark = (
        SparkSession.builder.appName("vn-news-bronze-to-silver")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(max(args.output_partitions * 2, 8)))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        bronze = spark.read.schema(NEWS_SCHEMA).option("recursiveFileLookup", "true").json(args.input)
        extract_tickers = F.udf(_extract_vn30_tickers, T.ArrayType(T.StringType(), containsNull=False))
        extract_ner = _build_ner_udf(str(Path(args.ner_model).resolve())) if args.ner_model else None
        clean_html = lambda column: F.trim(
            F.regexp_replace(F.regexp_replace(F.coalesce(column, F.lit("")), "<[^>]+>", " "), r"\s+", " ")
        )
        ranked = (
            bronze
            .withColumn("title", clean_html(F.col("title")))
            .withColumn("summary", clean_html(F.col("summary")))
            .withColumn("content", clean_html(F.col("content")))
            .withColumn("source", F.lower(F.trim(F.col("source"))))
            .withColumn("url", F.trim(F.col("url")))
            .withColumn("published_timestamp", F.to_timestamp("published_at_utc"))
            .withColumn("collected_timestamp", F.to_timestamp("collected_at_utc"))
            .withColumn("content_length", F.length("content"))
            .withColumn("has_full_text", F.col("content_length") >= 200)
            .withColumn(
                "_quality_rank",
                F.row_number().over(
                    Window.partitionBy("event_id").orderBy(
                        F.col("has_full_text").desc(),
                        F.col("content_length").desc(),
                        F.col("collected_timestamp").desc_nulls_last(),
                    )
                ),
            )
            .filter(F.col("_quality_rank") == 1)
            .drop("_quality_rank")
        )
        silver = (
            ranked
            .withColumn(
                "body_for_extraction",
                F.when(F.col("has_full_text"), F.col("content")).otherwise(F.col("summary")),
            )
            .withColumn(
                "text_for_extraction",
                F.trim(F.concat_ws("\n\n", F.col("title"), F.col("body_for_extraction"))),
            )
            .withColumn("content_hash", F.sha2(F.col("text_for_extraction"), 256))
            .withColumn("published_date", F.to_date(F.coalesce("published_timestamp", "collected_timestamp")))
            .withColumn("published_year_month", F.date_format("published_date", "yyyy-MM"))
            .withColumn("rulebased_tickers", extract_tickers("text_for_extraction"))
            .withColumn(
                "ner_tickers",
                extract_ner("text_for_extraction")
                if extract_ner is not None else F.array().cast(T.ArrayType(T.StringType())),
            )
            .withColumn("merged_tickers", F.array_distinct(F.concat("rulebased_tickers", "ner_tickers")))
            .withColumn(
                "ticker_extraction_strategy",
                F.lit("rulebased_vn30+crf_ner" if extract_ner is not None else "rulebased_vn30"),
            )
            .withColumn(
                "data_quality_errors",
                F.array_compact(F.array(
                    F.when(F.col("event_id").isNull(), F.lit("missing_event_id")),
                    F.when(F.length("title") == 0, F.lit("missing_title")),
                    F.when(
                        ~F.coalesce(F.col("url").rlike(r"^https?://"), F.lit(False)),
                        F.lit("invalid_url"),
                    ),
                    F.when(F.col("published_timestamp").isNull(), F.lit("invalid_published_at")),
                    F.when(F.col("collected_timestamp").isNull(), F.lit("invalid_collected_at")),
                    F.when(~F.col("schema_version").eqNullSafe(F.lit(1)), F.lit("unsupported_schema")),
                )),
            )
            .withColumn("is_valid", F.size("data_quality_errors") == 0)
            .drop("body_for_extraction")
            .cache()
        )
        total = silver.count()
        valid = silver.filter("is_valid").count()
        with_ticker = silver.filter(F.size("merged_tickers") > 0).count()
        ner_added = silver.filter(F.size(F.array_except("ner_tickers", "rulebased_tickers")) > 0).count()
        with_full_text = silver.filter("has_full_text").count()
        (
            silver.repartition(args.output_partitions, "published_year_month").write.mode(args.mode)
            .partitionBy("published_year_month")
            .parquet(args.output)
        )
        print(
            f"Silver rows={total} valid={valid} full_text={with_full_text} "
            f"with_vn30_ticker={with_ticker} ner_added_rows={ner_added} output={args.output}"
        )
        silver.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
