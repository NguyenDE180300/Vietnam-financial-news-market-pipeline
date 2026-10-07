"""Parallel rule-based + NER ticker extraction and dataset filtering."""
from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .ner_ticker import CRFTickerNER
from .ticker_extractor import TickerExtractor, VN30_TICKERS


@dataclass
class HybridTickerResult:
    rulebased_tickers: list[str]
    ner_tickers: list[str]
    merged_tickers: list[str]
    ner_entities: list[dict]


class HybridTickerExtractor:
    def __init__(self, model_path: str | Path):
        self.rule_extractor = TickerExtractor()
        self.ner_extractor = CRFTickerNER(model_path)
        self.vn30 = set(VN30_TICKERS)

    def extract_with_details(self, text: str) -> HybridTickerResult:
        rule_tickers = [ticker for ticker in self.rule_extractor.extract(text) if ticker in self.vn30]
        ner_result = self.ner_extractor.extract_with_entities(text)
        ner_tickers = [ticker for ticker in ner_result.tickers if ticker in self.vn30]
        merged = list(dict.fromkeys(rule_tickers + ner_tickers))
        return HybridTickerResult(rule_tickers, ner_tickers, merged, ner_result.entities)

    def extract(self, text: str) -> list[str]:
        return self.extract_with_details(text).merged_tickers


def export_filtered_dataset(db_path: str | Path, model_path: str | Path,
                            output_path: str | Path, limit: int = 0) -> dict[str, int]:
    extractor = HybridTickerExtractor(model_path)
    query = """SELECT id,title,summary,content,source,url,published_at,event_time
               FROM news_items ORDER BY id"""
    parameters: tuple = ()
    if limit > 0:
        query += " LIMIT ?"
        parameters = (limit,)
    with sqlite3.connect(db_path) as connection:
        rows = connection.execute(query, parameters).fetchall()
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    kept = 0
    rule_only = ner_added = 0
    with output.open("w", encoding="utf-8") as stream:
        for news_id, title, summary, content, source, url, published_at, event_time in rows:
            body = (content or summary or "").strip()
            text = f"{title or ''}\n\n{body}".strip()
            result = extractor.extract_with_details(text)
            if not result.merged_tickers:
                continue
            kept += 1
            rule_only += bool(result.rulebased_tickers and not result.ner_tickers)
            ner_added += bool(set(result.ner_tickers) - set(result.rulebased_tickers))
            record = {
                "news_id": news_id, "source": source, "url": url,
                "published_at": published_at, "event_time": event_time,
                "title": title, "content": body,
                "rulebased_tickers": result.rulebased_tickers,
                "ner_tickers": result.ner_tickers,
                "merged_tickers": result.merged_tickers,
                "ner_entities": result.ner_entities,
            }
            stream.write(json.dumps(record, ensure_ascii=False) + "\n")
    return {"scanned": len(rows), "kept": kept, "dropped": len(rows) - kept,
            "rule_only": rule_only, "ner_added_ticker": ner_added}


def main() -> None:
    parser = argparse.ArgumentParser(description="Export only news with a hybrid VN30 ticker")
    parser.add_argument("--db", default="news_system.db")
    parser.add_argument("--model", default="models/ticker_ner_crf.joblib")
    parser.add_argument("--output", default="data/news_with_vn30_tickers.jsonl")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--text", help="Test one piece of text instead of exporting SQLite")
    args = parser.parse_args()
    if args.text:
        result = HybridTickerExtractor(args.model).extract_with_details(args.text)
        print(json.dumps(result.__dict__, ensure_ascii=False, indent=2))
        return
    report = export_filtered_dataset(args.db, args.model, args.output, args.limit)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"Saved filtered dataset: {args.output}")


if __name__ == "__main__":
    main()
