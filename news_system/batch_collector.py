"""Collect incremental RSS batches and persist immutable Bronze JSONL files."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from .article_extractor import extract_article
from .lake_storage import create_lake_storage
from .models import NewsItem
from .rss_collector import DEFAULT_FEEDS, collect_feed


VN_TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
STATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS emitted_news (
 event_id TEXT PRIMARY KEY,
 source TEXT NOT NULL,
 url TEXT,
 emitted_at_utc TEXT NOT NULL,
 bronze_path TEXT NOT NULL
);
"""


def news_event_id(item: NewsItem) -> str:
    # A URL can appear in more than one feed/category. Use it as the global
    # identity so the same article is emitted only once across sources.
    identity = item.url or f"{item.source}|{item.title}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed


def bronze_record(item: NewsItem, collected_at: datetime) -> dict:
    published = _parse_time(item.published_at)
    return {
        "event_id": news_event_id(item),
        "schema_version": 1,
        "event_type": "news.collected",
        "source": item.source,
        "url": item.url,
        "title": item.title,
        "summary": item.summary,
        "content": item.content,
        "author": item.author,
        "image_url": item.image_url,
        "published_at_raw": item.published_at,
        "published_at_utc": published.astimezone(timezone.utc).isoformat() if published else None,
        "published_at_vn": published.astimezone(VN_TIMEZONE).isoformat() if published else None,
        "published_precision": item.published_precision,
        "collected_at_utc": collected_at.astimezone(timezone.utc).isoformat(),
        "ingest_date": collected_at.astimezone(timezone.utc).date().isoformat(),
    }


class CollectorState:
    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        with sqlite3.connect(self.path) as connection:
            connection.executescript(STATE_SCHEMA)

    def unseen(self, items: list[NewsItem]) -> list[NewsItem]:
        if not items:
            return []
        ids = [news_event_id(item) for item in items]
        placeholders = ",".join("?" for _ in ids)
        with sqlite3.connect(self.path) as connection:
            existing = {
                row[0] for row in connection.execute(
                    f"SELECT event_id FROM emitted_news WHERE event_id IN ({placeholders})", ids
                )
            }
        return [item for item, event_id in zip(items, ids) if event_id not in existing]

    def mark_emitted(self, items: list[NewsItem], bronze_path: str, emitted_at: datetime) -> None:
        with sqlite3.connect(self.path) as connection:
            connection.executemany(
                "INSERT OR IGNORE INTO emitted_news VALUES (?,?,?,?,?)",
                [
                    (news_event_id(item), item.source, item.url or None,
                     emitted_at.astimezone(timezone.utc).isoformat(), bronze_path)
                    for item in items
                ],
            )


def collect_batch(
    storage,
    state: CollectorState,
    full_text: bool = True,
    article_limit: int = 20,
    feeds: dict[str, str] = DEFAULT_FEEDS,
) -> tuple[str | None, int]:
    candidates: list[NewsItem] = []
    for source, feed_url in feeds.items():
        try:
            items = collect_feed(feed_url, source)
            candidates.extend(state.unseen(items))
            print(f"{source}: fetched={len(items)}", flush=True)
        except Exception as error:
            print(f"[WARN] RSS {source}: {error}", flush=True)
    unseen_by_id = {news_event_id(item): item for item in candidates}
    unseen = list(unseen_by_id.values())
    if not unseen:
        print("No new articles.", flush=True)
        return None, 0

    fetched = 0
    if full_text:
        for item in unseen:
            if article_limit > 0 and fetched >= article_limit:
                break
            if not item.url:
                continue
            try:
                article = extract_article(item.url)
                if article.title:
                    item.title = article.title
                if article.content:
                    item.content = article.content
                item.author = article.author
                item.image_url = article.image_url or item.image_url
                fetched += 1
                time.sleep(0.5)
            except Exception as error:
                print(f"[WARN] article {item.url}: {error}", flush=True)

    collected_at = datetime.now(timezone.utc)
    records = [bronze_record(item, collected_at) for item in unseen]
    payload = "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records)
    compressed = gzip.compress(payload.encode("utf-8"), compresslevel=6)
    stamp = collected_at.strftime("%Y%m%dT%H%M%S%fZ")
    relative_path = (
        f"news/ingest_date={collected_at:%Y-%m-%d}/hour={collected_at:%H}/"
        f"batch_{stamp}.jsonl.gz"
    )
    bronze_path = storage.write_bytes("bronze", relative_path, compressed)
    state.mark_emitted(unseen, bronze_path, collected_at)
    print(f"Bronze batch: records={len(records)} full_text={fetched} path={bronze_path}", flush=True)
    return bronze_path, len(records)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=("local", "adls"), default="local")
    parser.add_argument("--root", default="data_lake")
    parser.add_argument("--account-name", help="ADLS account; defaults to AZURE_STORAGE_ACCOUNT")
    parser.add_argument("--state-db", default="collector_state.db")
    parser.add_argument("--no-full-text", action="store_true")
    parser.add_argument("--article-limit", type=int, default=0,
                        help="Maximum full articles per run; 0 fetches every new article")
    args = parser.parse_args()
    storage = create_lake_storage(args.backend, args.root, args.account_name)
    state = CollectorState(args.state_db)
    collect_batch(
        storage, state, full_text=not args.no_full_text,
        article_limit=max(0, args.article_limit),
    )


if __name__ == "__main__":
    main()
