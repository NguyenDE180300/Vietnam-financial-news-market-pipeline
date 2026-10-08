"""Export historical SQLite news into immutable Bronze JSONL.GZ shards."""
from __future__ import annotations

import argparse
import gzip
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .batch_collector import bronze_record
from .lake_storage import create_lake_storage
from .models import NewsItem


def _aware_time(*values: str | None) -> datetime:
    for value in values:
        if not value:
            continue
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            continue
    return datetime.now(timezone.utc)


def export_sqlite(
    db_path: str | Path,
    storage,
    batch_size: int = 1000,
) -> tuple[int, int]:
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    query = """SELECT id,title,summary,source,url,published_at,event_time,
                      published_precision,created_at,observed_at,content,author,image_url,
                      tickers_json,rulebased_tickers_json,ner_tickers_json,
                      ticker_extraction_strategy
               FROM news_items WHERE tickers_json != '[]' ORDER BY id"""
    exported = shards = 0
    with sqlite3.connect(db_path) as connection:
        cursor = connection.execute(query)
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            records = []
            for row in rows:
                (news_id, title, summary, source, url, published_at, event_time,
                 precision, created_at, observed_at, content, author, image_url,
                 tickers_json, rule_json, ner_json, strategy) = row
                item = NewsItem(
                    title=title or "", summary=summary or "", source=source or "",
                    url=url or "", published_at=event_time or published_at,
                    event_time=event_time or published_at,
                    published_precision=precision or "unknown", content=content or "",
                    author=author, image_url=image_url,
                    tickers=json.loads(tickers_json or "[]"),
                    rulebased_tickers=json.loads(rule_json or "[]"),
                    ner_tickers=json.loads(ner_json or "[]"),
                    ticker_extraction_strategy=strategy or "rulebased",
                )
                record = bronze_record(item, _aware_time(created_at, observed_at))
                record["event_type"] = "news.historical.exported"
                record["legacy_news_id"] = int(news_id)
                records.append(record)
            payload = "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records)
            first_id, last_id = rows[0][0], rows[-1][0]
            relative = f"news_stream/history/sqlite_{first_id:08d}_{last_id:08d}.jsonl.gz"
            path = storage.write_bytes("bronze", relative, gzip.compress(payload.encode(), 6))
            exported += len(records)
            shards += 1
            print(f"Historical Bronze shard: records={len(records)} path={path}", flush=True)
    print(f"Historical export complete: records={exported} shards={shards}", flush=True)
    return exported, shards


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="news_system.db")
    parser.add_argument("--backend", choices=("local", "adls"), default="local")
    parser.add_argument("--root", default="data_lake")
    parser.add_argument("--account-name", help="ADLS account; defaults to AZURE_STORAGE_ACCOUNT")
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()
    storage = create_lake_storage(args.backend, args.root, args.account_name)
    export_sqlite(args.db, storage, args.batch_size)


if __name__ == "__main__":
    main()
