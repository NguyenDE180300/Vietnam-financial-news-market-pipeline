"""Build a clean, manually-labelled ticker dataset from collected VN news.

The collector remains responsible for RSS ingestion. This module creates a
stable annotation file: one row per deduplicated article, with gold ticker
columns intentionally left blank for manual annotation.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .database import NewsDatabase
from .pipeline import collect_news
from .ticker_extractor import VN30_TICKERS


OUTPUT_COLUMNS = [
    "annotation_id", "news_id", "source", "url", "published_at_raw",
    "published_at_utc", "published_at_vn", "title", "summary", "content",
    "text_for_labeling", "predicted_tickers", "rulebased_tickers",
    "manual_tickers", "merged_tickers", "gold_tickers", "entity_spans_json",
    "label_status", "label_notes", "content_hash",
]


def _to_utc(value: str | None) -> str:
    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    except ValueError:
        return ""


def _to_vietnam(value: str | None) -> str:
    if not value:
        return ""
    try:
        from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).isoformat()
    except (ValueError, ZoneInfoNotFoundError):
        return value


def build_rows(db_path: str, limit: int = 5000) -> list[dict[str, str]]:
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        records = connection.execute(
            """SELECT id,source,url,published_at,title,summary,content,tickers_json,content_hash
               FROM news_items
               WHERE title IS NOT NULL AND TRIM(title) <> ''
               ORDER BY COALESCE(published_at, created_at), id
               LIMIT ?""", (limit,)
        ).fetchall()

    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for record in records:
        title = (record["title"] or "").strip()
        summary = (record["summary"] or "").strip()
        content = (record["content"] or "").strip()
        text = "\n\n".join(part for part in (title, summary, content) if part)
        # Prefer the normalized content hash over URL: Google News may expose
        # the same article through different redirect URLs.
        identity = record["content_hash"] or record["url"] or hashlib.sha256(text.encode()).hexdigest()
        if identity in seen:
            continue
        seen.add(identity)
        predicted = json.loads(record["tickers_json"] or "[]")
        rows.append({
            "annotation_id": hashlib.sha1(identity.encode()).hexdigest()[:16],
            "news_id": str(record["id"]),
            "source": record["source"] or "",
            "url": record["url"] or "",
            "published_at_raw": record["published_at"] or "",
            "published_at_utc": _to_utc(record["published_at"]),
            "published_at_vn": _to_vietnam(record["published_at"]),
            "title": title,
            "summary": summary,
            "content": content,
            "text_for_labeling": text,
            "predicted_tickers": json.dumps(predicted, ensure_ascii=False),
            "rulebased_tickers": json.dumps(predicted, ensure_ascii=False),
            "manual_tickers": "",
            "merged_tickers": "",
            "gold_tickers": "",
            "entity_spans_json": "[]",
            "label_status": "unlabeled",
            "label_notes": "",
            "content_hash": record["content_hash"] or hashlib.sha256(text.encode()).hexdigest(),
        })
    return rows


def write_csv(rows: list[dict[str, str]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.DictWriter(stream, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def write_jsonl(rows: list[dict[str, str]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare a manually labelled VN ticker dataset")
    parser.add_argument("--db", default="news_system.db")
    parser.add_argument("--output", default="data/ticker_dataset.csv")
    parser.add_argument("--jsonl", default="data/ticker_dataset.jsonl")
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--collect", action="store_true", help="Run one RSS collection before export")
    parser.add_argument("--full-text", action="store_true", help="Fetch article pages before export")
    parser.add_argument("--article-limit", type=int, default=0)
    parser.add_argument("--vn30-only", action="store_true",
                        help="Keep only articles whose predicted ticker intersects the VN30 snapshot")
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("--limit must be positive")
    database = NewsDatabase(args.db)
    if args.collect:
        inserted = collect_news(database, full_text=args.full_text, article_limit=args.article_limit)
        print(f"RSS collection inserted={inserted}")
    rows = build_rows(args.db, args.limit)
    if args.vn30_only:
        universe = set(VN30_TICKERS)
        rows = [row for row in rows if universe.intersection(json.loads(row["predicted_tickers"] or "[]"))]
    write_csv(rows, Path(args.output))
    write_jsonl(rows, Path(args.jsonl))
    print(f"Exported {len(rows)} deduplicated articles")
    if len(rows) < args.limit:
        print(f"WARNING: target={args.limit}, missing={args.limit - len(rows)}")
        print("RSS feeds are not historical archives; collect over time or run historical_backfill.")
    print(f"CSV: {args.output}")
    print(f"JSONL: {args.jsonl}")
    print("Fill gold_tickers/entity_spans_json and change label_status to labeled.")


if __name__ == "__main__":
    main()
