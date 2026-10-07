"""Run the local daily Market -> Silver -> Gold pipeline with quality gates."""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="data_lake")
    parser.add_argument("--period", default="1mo")
    parser.add_argument("--ner-model", default="models/ticker_ner_crf.joblib")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    spark_submit = shutil.which("spark-submit")
    if not spark_submit:
        candidate = Path(sys.executable).with_name("spark-submit")
        if candidate.exists():
            spark_submit = str(candidate)
    if not spark_submit:
        raise SystemExit("spark-submit was not found in PATH or the active Conda environment")

    root = Path(args.root)
    environment = os.environ.copy()
    environment["PYSPARK_PYTHON"] = sys.executable
    environment["PYSPARK_DRIVER_PYTHON"] = sys.executable

    commands = [
        [sys.executable, "-m", "news_system.market_batch_collector", "--backend", "local",
         "--root", str(root), "--provider", "yahoo", "--vn30", "--period", args.period],
        [spark_submit, "news_system/spark_jobs/bronze_to_silver.py",
         "--input", str(root / "bronze/news_stream"),
         "--output", str(root / "silver/news"), "--mode", "overwrite",
         "--output-partitions", "4", "--ner-model", args.ner_model],
        [spark_submit, "news_system/spark_jobs/validate_silver.py",
         "--input", str(root / "silver/news")],
        [spark_submit, "news_system/spark_jobs/market_bronze_to_silver.py",
         "--input", str(root / "bronze/market"),
         "--output", str(root / "silver/market_daily"), "--mode", "overwrite",
         "--output-partitions", "4"],
        [spark_submit, "news_system/spark_jobs/validate_market_silver.py",
         "--input", str(root / "silver/market_daily"), "--expected-min-tickers", "30"],
        [spark_submit, "news_system/spark_jobs/news_market_gold.py",
         "--news-input", str(root / "silver/news"),
         "--market-input", str(root / "silver/market_daily"),
         "--output", str(root / "gold/news_market_impact"), "--mode", "overwrite",
         "--output-partitions", "4"],
        [spark_submit, "news_system/spark_jobs/validate_news_market_gold.py",
         "--input", str(root / "gold/news_market_impact")],
    ]
    for command in commands:
        print("+", " ".join(command), flush=True)
        if not args.dry_run:
            subprocess.run(command, check=True, env=environment)
    print("Daily pipeline PASSED", flush=True)


if __name__ == "__main__":
    main()
