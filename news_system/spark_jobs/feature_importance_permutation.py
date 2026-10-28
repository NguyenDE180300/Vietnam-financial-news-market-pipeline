"""Permutation importance for trained full-content Spark logistic models.

Importance is measured as the drop in Macro-F1 after permuting one raw
feature/group across rows in a held-out temporal split. PhoBERT embeddings are
permuted as complete vectors, not coordinate by coordinate.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.functions import array_to_vector
from pyspark.sql import SparkSession, functions as F
from sklearn.metrics import accuracy_score, f1_score

from news_system.spark_jobs.train_movement_baseline import _prepare


MARKET_FEATURES = [
    "market_return_1d",
    "market_intraday_range",
    "market_open_gap",
    "market_log1p_volume",
    "market_volume_ratio_20",
    "market_momentum_5",
    "market_momentum_20",
    "market_volatility_20",
]
HISTORY_FEATURES = [
    "history_available_numeric",
    "history_count",
    "days_since_last_history",
    "history_mean_similarity",
    "history_max_similarity",
]
HORIZONS = (1, 3, 5)


def split_features(feature_set: str, text_width: int, row_width: int) -> list[tuple[str, slice]]:
    groups: list[tuple[str, slice]] = [("current_text_embedding", slice(0, text_width))]
    offset = text_width
    if feature_set == "text_market_history":
        groups.append(("history_text_embedding", slice(offset, offset + text_width)))
        offset += text_width
    for name in MARKET_FEATURES:
        groups.append((name, slice(offset, offset + 1)))
        offset += 1
    if feature_set == "text_market_history":
        for name in HISTORY_FEATURES:
            groups.append((name, slice(offset, offset + 1)))
            offset += 1
    if offset != row_width:
        raise ValueError(f"Unexpected feature width: expected {offset}, got {row_width}")
    return groups


def predict_from_coefficients(X: np.ndarray, model: LogisticRegressionModel) -> np.ndarray:
    coefficients = np.asarray(model.coefficientMatrix.toArray(), dtype=np.float64)
    intercept = np.asarray(model.interceptVector.toArray(), dtype=np.float64)
    logits = X @ coefficients.T + intercept
    return np.argmax(logits, axis=1).astype(np.int32)


def evaluate_permutation(
    X: np.ndarray,
    y: np.ndarray,
    model: LogisticRegressionModel,
    groups: list[tuple[str, slice]],
    repeats: int,
    seed: int,
) -> dict:
    baseline_pred = predict_from_coefficients(X, model)
    baseline_macro = float(f1_score(y, baseline_pred, labels=[0, 1], average="macro", zero_division=0))
    baseline_accuracy = float(accuracy_score(y, baseline_pred))
    rows = []
    rng = np.random.default_rng(seed)
    for name, feature_slice in groups:
        macro_scores, accuracy_scores = [], []
        for _ in range(repeats):
            permutation = rng.permutation(len(y))
            permuted = X.copy()
            permuted[:, feature_slice] = X[permutation, feature_slice]
            predictions = predict_from_coefficients(permuted, model)
            macro_scores.append(float(f1_score(
                y, predictions, labels=[0, 1], average="macro", zero_division=0,
            )))
            accuracy_scores.append(float(accuracy_score(y, predictions)))
        drops = baseline_macro - np.asarray(macro_scores)
        accuracy_drops = baseline_accuracy - np.asarray(accuracy_scores)
        rows.append({
            "feature": name,
            "macro_f1_drop_mean": float(drops.mean()),
            "macro_f1_drop_std": float(drops.std(ddof=1)) if repeats > 1 else 0.0,
            "permuted_macro_f1_mean": float(np.mean(macro_scores)),
            "accuracy_drop_mean": float(accuracy_drops.mean()),
            "permuted_accuracy_mean": float(np.mean(accuracy_scores)),
            "repeats": repeats,
        })
    rows.sort(key=lambda item: item["macro_f1_drop_mean"], reverse=True)
    return {
        "rows": int(len(y)),
        "baseline_macro_f1": baseline_macro,
        "baseline_accuracy": baseline_accuracy,
        "importance": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--repeats", type=int, default=30)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("--repeats must be >= 1")

    root = Path(__file__).resolve().parents[2]
    dataset = str((root / args.input).resolve())
    output_path = (root / args.output).resolve()
    specs = [
        {
            "name": "fixed_fulltext_market_history",
            "model": "models/ablation_binary_fulltext_text_market_history_v1",
            "feature_set": "text_market_history",
            "train_end": "2024-12-31",
            "validation_end": "2025-12-31",
        },
        {
            "name": "expanded_fulltext_market",
            "model": "models/ablation_binary_fulltext_rolling_v2_text_market",
            "feature_set": "text_market",
            "train_end": "2025-12-31",
            "validation_end": "2026-04-30",
        },
        {
            "name": "expanded_fulltext_market_history",
            "model": "models/ablation_binary_fulltext_rolling_v2_text_market_history",
            "feature_set": "text_market_history",
            "train_end": "2025-12-31",
            "validation_end": "2026-04-30",
        },
    ]

    spark = (
        SparkSession.builder.appName("vn-news-market-permutation-importance")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    results = {
        "method": "held-out row permutation; importance = baseline macro-F1 - permuted macro-F1",
        "repeats": args.repeats,
        "seed": args.seed,
        "label_order": {"0": "decrease", "1": "increase"},
        "embedding_note": "Current/history PhoBERT embedding permuted as a complete vector.",
        "runs": [],
    }
    try:
        raw = spark.read.parquet(dataset)
        raw = raw.withColumn("current_phobert_vector", array_to_vector("current_phobert_embedding"))
        if "history_phobert_embedding" in raw.columns:
            raw = raw.withColumn("history_phobert_vector", array_to_vector("history_phobert_embedding"))
        prepared = _prepare(raw).cache()

        for spec in specs:
            preprocessor = PipelineModel.load(str(root / spec["model"] / "preprocessor"))
            transformed = preprocessor.transform(prepared).select(
                "event_date", "features", "stock_return_t1", "mask_t1",
                "stock_return_t3", "mask_t3", "stock_return_t5", "mask_t5",
            ).cache()
            date = F.col("event_date")
            split_filters = {
                "validation": (date > F.lit(spec["train_end"]).cast("date"))
                & (date <= F.lit(spec["validation_end"]).cast("date")),
                "test": date > F.lit(spec["validation_end"]).cast("date"),
            }
            head_results = {}
            for horizon in HORIZONS:
                model = LogisticRegressionModel.load(
                    str(root / spec["model"] / f"head_t{horizon}")
                )
                head_results[f"t{horizon}"] = {}
                for split_name, split_filter in split_filters.items():
                    eligible = transformed.filter(
                        split_filter
                        & F.col(f"mask_t{horizon}")
                        & F.col(f"stock_return_t{horizon}").isNotNull()
                        & (F.col(f"stock_return_t{horizon}") != 0.0)
                    ).select("features", f"stock_return_t{horizon}").collect()
                    if not eligible:
                        continue
                    X = np.vstack([np.asarray(row["features"].toArray(), dtype=np.float64) for row in eligible])
                    y = np.asarray([
                        1 if float(row[f"stock_return_t{horizon}"]) > 0 else 0
                        for row in eligible
                    ], dtype=np.int32)
                    width = X.shape[1]
                    groups = split_features(spec["feature_set"], 768, width)

                    # Check the local coefficient implementation against Spark's predictions.
                    spark_predictions = model.transform(
                        spark.createDataFrame([(row["features"],) for row in eligible], ["features"])
                    ).select("prediction").collect()
                    expected = np.asarray([int(row.prediction) for row in spark_predictions])
                    actual = predict_from_coefficients(X, model)
                    if not np.array_equal(expected, actual):
                        raise RuntimeError(f"Coefficient prediction mismatch for {spec['name']} t{horizon}")

                    head_results[f"t{horizon}"][split_name] = evaluate_permutation(
                        X, y, model, groups, args.repeats,
                        args.seed + horizon * 100 + (0 if split_name == "validation" else 1),
                    )
                print(f"Finished {spec['name']} t{horizon}", flush=True)
            transformed.unpersist()
            results["runs"].append({
                "name": spec["name"],
                "feature_set": spec["feature_set"],
                "temporal_split": {
                    "train_end": spec["train_end"],
                    "validation_end": spec["validation_end"],
                },
                "heads": head_results,
            })
        prepared.unpersist()
    finally:
        spark.stop()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
