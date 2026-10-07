"""Resolve Google News URLs and enrich saved news with publisher articles."""
from __future__ import annotations

import argparse
import sqlite3
import time
from datetime import datetime, timezone


def ensure_columns(db_path: str) -> None:
    with sqlite3.connect(db_path) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(news_items)")}
        for name, definition in {
            "resolved_url": "TEXT",
            "extraction_status": "TEXT",
            "extraction_method": "TEXT",
            "content_length": "INTEGER",
            "extraction_error": "TEXT",
        }.items():
            if name not in columns:
                connection.execute(f"ALTER TABLE news_items ADD COLUMN {name} {definition}")


def decode_url(url: str) -> str:
    if not url.startswith("https://news.google.com/"):
        return url
    from googlenewsdecoder import gnewsdecoder
    result = gnewsdecoder(url, interval=0.5)
    if not result.get("status"):
        raise RuntimeError(str(result))
    return result["decoded_url"]


def extract(url: str):
    from newspaper import article
    item = article(url)
    if not item.text or len(item.text.strip()) < 200:
        raise RuntimeError(f"content too short ({len(item.text.strip())} chars)")
    return item


def crawl(db_path: str, limit: int, delay: float) -> tuple[int, int, int]:
    ensure_columns(db_path)
    with sqlite3.connect(db_path) as connection:
        rows = connection.execute(
            """SELECT id,url FROM news_items
               WHERE (content IS NULL OR length(content) < 200)
                 AND (extraction_status IS NULL OR extraction_status NOT IN ('success','skipped'))
               ORDER BY id LIMIT ?""", (limit,)
        ).fetchall()
    success = failed = skipped = 0
    for news_id, original_url in rows:
        if not original_url:
            skipped += 1
            continue
        try:
            resolved = decode_url(original_url)
            item = extract(resolved)
            with sqlite3.connect(db_path) as connection:
                connection.execute(
                    """UPDATE news_items SET content=?, author=?, image_url=?,
                       resolved_url=?, extraction_status='success', extraction_method=?,
                       content_length=?, extraction_error=NULL WHERE id=?""",
                    (item.text, ", ".join(item.authors) if item.authors else None,
                     item.top_image or None, resolved, "google_decoder+newspaper4k",
                     len(item.text), news_id),
                )
            success += 1
            print(f"{news_id}: success chars={len(item.text)} {resolved}")
        except Exception as error:
            with sqlite3.connect(db_path) as connection:
                connection.execute(
                    """UPDATE news_items SET resolved_url=?, extraction_status='failed',
                       extraction_method='google_decoder+newspaper4k', content_length=0,
                       extraction_error=? WHERE id=?""",
                    (None, str(error)[:500], news_id),
                )
            failed += 1
            print(f"{news_id}: failed {type(error).__name__}: {error}")
        time.sleep(max(0.0, delay))
    return success, failed, skipped


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="news_system.db")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--delay", type=float, default=1.0)
    args = parser.parse_args()
    result = crawl(args.db, args.limit, args.delay)
    print(f"completed success={result[0]} failed={result[1]} skipped={result[2]}")


if __name__ == "__main__":
    main()
