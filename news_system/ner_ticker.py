"""Train and run a lightweight Vietnamese ticker NER baseline.

The CRF is intentionally a temporary, non-LLM baseline.  It predicts entity
spans while a separate linker maps each mention to a VN30 ticker.  The bundle
can later be replaced by a PhoBERT token-classification model without changing
the hybrid extraction interface.
"""
from __future__ import annotations

import argparse
import json
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

import joblib
import pandas as pd
import sklearn_crfsuite
from rapidfuzz import fuzz, process
from sklearn.model_selection import train_test_split

from .ticker_extractor import DEFAULT_ALIASES, VN30_TICKERS


TOKEN_RE = re.compile(r"\w+(?:[.-]\w+)*|[^\w\s]", re.UNICODE)


def normalize_mention(text: str) -> str:
    text = unicodedata.normalize("NFD", text.casefold())
    text = "".join(char for char in text if unicodedata.category(char) != "Mn")
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def tokenize_with_offsets(text: str) -> list[tuple[str, int, int]]:
    return [(match.group(), match.start(), match.end()) for match in TOKEN_RE.finditer(text or "")]


def _token_features(tokens: list[str], index: int) -> dict[str, object]:
    token = tokens[index]
    lower = token.casefold()
    features: dict[str, object] = {
        "bias": 1.0,
        "word.lower": lower,
        "word[-3:]": lower[-3:],
        "word[-2:]": lower[-2:],
        "word[:3]": lower[:3],
        "word.isupper": token.isupper(),
        "word.istitle": token.istitle(),
        "word.isdigit": token.isdigit(),
        "word.has_digit": any(char.isdigit() for char in token),
        "word.length": min(len(token), 20),
    }
    if index > 0:
        previous = tokens[index - 1]
        features.update({
            "-1:word.lower": previous.casefold(),
            "-1:word.isupper": previous.isupper(),
            "-1:word.istitle": previous.istitle(),
        })
    else:
        features["BOS"] = True
    if index < len(tokens) - 1:
        following = tokens[index + 1]
        features.update({
            "+1:word.lower": following.casefold(),
            "+1:word.isupper": following.isupper(),
            "+1:word.istitle": following.istitle(),
        })
    else:
        features["EOS"] = True
    return features


def sentence_features(offset_tokens: list[tuple[str, int, int]]) -> list[dict[str, object]]:
    tokens = [token for token, _, _ in offset_tokens]
    return [_token_features(tokens, index) for index in range(len(tokens))]


def spans_to_bio(offset_tokens: list[tuple[str, int, int]], spans: list[dict]) -> list[str]:
    labels = ["O"] * len(offset_tokens)
    for span in sorted(spans, key=lambda item: (int(item["start"]), int(item["end"]))):
        overlapping = [
            index for index, (_, start, end) in enumerate(offset_tokens)
            if start < int(span["end"]) and end > int(span["start"])
        ]
        for position, index in enumerate(overlapping):
            labels[index] = "B-ORG" if position == 0 else "I-ORG"
    return labels


def _load_examples(csv_path: str | Path) -> tuple[list[dict], dict[str, str]]:
    frame = pd.read_csv(csv_path).fillna("")
    valid_tickers = set(VN30_TICKERS)
    mention_counts: dict[str, Counter] = defaultdict(Counter)
    examples = []
    for row in frame.to_dict("records"):
        text = str(row.get("text_for_labeling") or "").strip()
        if not text:
            continue
        try:
            raw_spans = json.loads(row.get("entity_spans_json") or "[]")
        except (TypeError, json.JSONDecodeError):
            raw_spans = []
        spans = []
        for span in raw_spans:
            ticker = str(span.get("ticker", "")).upper()
            start, end = int(span.get("start", -1)), int(span.get("end", -1))
            if ticker not in valid_tickers or start < 0 or end <= start or text[start:end] != span.get("text"):
                continue
            clean_span = {"text": span["text"], "ticker": ticker, "start": start, "end": end}
            spans.append(clean_span)
            mention_counts[normalize_mention(span["text"])][ticker] += 1
        offset_tokens = tokenize_with_offsets(text)
        examples.append({
            "text": text,
            "tokens": offset_tokens,
            "features": sentence_features(offset_tokens),
            "labels": spans_to_bio(offset_tokens, spans),
        })
    mention_to_ticker = {
        mention: counts.most_common(1)[0][0]
        for mention, counts in mention_counts.items() if mention
    }
    return examples, mention_to_ticker


def _entity_ranges(labels: list[str]) -> set[tuple[int, int]]:
    entities: set[tuple[int, int]] = set()
    start: int | None = None
    for index, label in enumerate(labels + ["O"]):
        if label == "B-ORG" or (label == "I-ORG" and start is None):
            if start is not None:
                entities.add((start, index))
            start = index
        elif label == "O" and start is not None:
            entities.add((start, index))
            start = None
    return entities


def entity_metrics(gold: list[list[str]], predicted: list[list[str]]) -> dict[str, float | int]:
    true_positive = false_positive = false_negative = 0
    for expected, actual in zip(gold, predicted):
        gold_entities = _entity_ranges(expected)
        predicted_entities = _entity_ranges(actual)
        true_positive += len(gold_entities & predicted_entities)
        false_positive += len(predicted_entities - gold_entities)
        false_negative += len(gold_entities - predicted_entities)
    precision = true_positive / (true_positive + false_positive) if true_positive + false_positive else 0.0
    recall = true_positive / (true_positive + false_negative) if true_positive + false_negative else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "precision": round(precision, 4), "recall": round(recall, 4), "f1": round(f1, 4),
        "true_positive": true_positive, "false_positive": false_positive,
        "false_negative": false_negative,
    }


def _new_crf(max_iterations: int) -> sklearn_crfsuite.CRF:
    return sklearn_crfsuite.CRF(
        algorithm="lbfgs", c1=0.1, c2=0.1, max_iterations=max_iterations,
        all_possible_transitions=True,
    )


def train_bundle(csv_path: str | Path, output_path: str | Path,
                 max_iterations: int = 80, seed: int = 42) -> dict:
    examples, mention_to_ticker = _load_examples(csv_path)
    indices = list(range(len(examples)))
    train_indices, validation_indices = train_test_split(indices, test_size=0.2, random_state=seed)
    evaluation_model = _new_crf(max_iterations)
    evaluation_model.fit([examples[i]["features"] for i in train_indices],
                         [examples[i]["labels"] for i in train_indices])
    gold = [examples[i]["labels"] for i in validation_indices]
    predicted = evaluation_model.predict([examples[i]["features"] for i in validation_indices])
    metrics = entity_metrics(gold, predicted)

    production_model = _new_crf(max_iterations)
    production_model.fit([item["features"] for item in examples],
                         [item["labels"] for item in examples])
    alias_to_ticker = {}
    for ticker in VN30_TICKERS:
        for alias in DEFAULT_ALIASES.get(ticker, [ticker]):
            alias_to_ticker[normalize_mention(alias)] = ticker
    bundle = {
        "model_type": "crf_ner_v1",
        "model": production_model,
        "mention_to_ticker": mention_to_ticker,
        "alias_to_ticker": alias_to_ticker,
        "vn30_tickers": VN30_TICKERS,
        "metrics": metrics,
        "training_rows": len(examples),
        "validation_rows": len(validation_indices),
    }
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, output)
    return {key: value for key, value in bundle.items() if key != "model" and not key.endswith("to_ticker")}


@dataclass
class NERResult:
    tickers: list[str]
    entities: list[dict]


class CRFTickerNER:
    def __init__(self, model_path: str | Path):
        bundle = joblib.load(model_path)
        self.model = bundle["model"]
        self.mention_to_ticker = bundle["mention_to_ticker"]
        self.alias_to_ticker = bundle["alias_to_ticker"]
        self.known_mentions = list({**self.alias_to_ticker, **self.mention_to_ticker})

    def _link(self, mention: str) -> tuple[str | None, float]:
        normalized = normalize_mention(mention)
        if normalized in self.mention_to_ticker:
            return self.mention_to_ticker[normalized], 1.0
        if normalized in self.alias_to_ticker:
            return self.alias_to_ticker[normalized], 1.0
        if len(normalized) < 3:
            return None, 0.0
        match = process.extractOne(normalized, self.known_mentions, scorer=fuzz.ratio, score_cutoff=90)
        if not match:
            return None, 0.0
        candidate, score, _ = match
        ticker = self.mention_to_ticker.get(candidate) or self.alias_to_ticker.get(candidate)
        return ticker, score / 100

    def extract_with_entities(self, text: str) -> NERResult:
        offset_tokens = tokenize_with_offsets(text)
        if not offset_tokens:
            return NERResult([], [])
        labels = self.model.predict_single(sentence_features(offset_tokens))
        entities = []
        start_index: int | None = None
        for index, label in enumerate(labels + ["O"]):
            if label == "B-ORG" or (label == "I-ORG" and start_index is None):
                if start_index is not None:
                    entities.append((start_index, index))
                start_index = index
            elif label == "O" and start_index is not None:
                entities.append((start_index, index))
                start_index = None
        linked = []
        for first, after_last in entities:
            start = offset_tokens[first][1]
            end = offset_tokens[after_last - 1][2]
            mention = text[start:end]
            ticker, confidence = self._link(mention)
            linked.append({
                "text": mention, "start": start, "end": end,
                "ticker": ticker, "link_confidence": round(confidence, 3),
            })
        tickers = list(dict.fromkeys(entity["ticker"] for entity in linked if entity["ticker"]))
        return NERResult(tickers, linked)

    def extract(self, text: str) -> list[str]:
        return self.extract_with_entities(text).tickers


def main() -> None:
    parser = argparse.ArgumentParser(description="Train the temporary CRF ticker NER model")
    parser.add_argument("--data", default="data/labeled_data/ticker_dataset_labeled_rulebase_plus_assistant.csv")
    parser.add_argument("--output", default="models/ticker_ner_crf.joblib")
    parser.add_argument("--max-iterations", type=int, default=80)
    args = parser.parse_args()
    report = train_bundle(args.data, args.output, args.max_iterations)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"Saved model: {args.output}")


if __name__ == "__main__":
    main()
