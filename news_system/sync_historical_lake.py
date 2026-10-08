"""Export enriched historical news and rebuild local News Silver and Gold."""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

from .export_sqlite_to_bronze import export_sqlite
from .lake_storage import LocalLakeStorage


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="news_system.db")
    parser.add_argument("--root", default="data_lake")
    parser.add_argument("--ner-model", default="models/ticker_ner_crf.joblib")
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()

    root = Path(args.root).resolve()
    export_sqlite(args.db, LocalLakeStorage(root), args.batch_size)

    spark_submit = shutil.which("spark-submit")
    if not spark_submit:
        candidate = Path(sys.executable).with_name("spark-submit")
        if candidate.exists():
            spark_submit = str(candidate)
    if not spark_submit:
        raise SystemExit("spark-submit was not found in the active environment")

    environment = os.environ.copy()
    environment["PYSPARK_PYTHON"] = sys.executable
    environment["PYSPARK_DRIVER_PYTHON"] = sys.executable
    commands = [
        [spark_submit, "news_system/spark_jobs/bronze_to_silver.py",
         "--input", str(root / "bronze/news_stream"),
         "--output", str(root / "silver/news"), "--mode", "overwrite",
         "--output-partitions", "4", "--ner-model", args.ner_model],
        [spark_submit, "news_system/spark_jobs/validate_silver.py",
         "--input", str(root / "silver/news")],
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
        subprocess.run(command, check=True, env=environment)
    print("Historical lake sync PASSED", flush=True)


if __name__ == "__main__":
    main()
