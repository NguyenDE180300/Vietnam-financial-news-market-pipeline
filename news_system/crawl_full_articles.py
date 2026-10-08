"""Resolve Google News URLs and enrich saved news with publisher articles."""
from __future__ import annotations

import argparse
import json
import sqlite3
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from .models import NewsItem
from .news_ingestion import extract_item_tickers


def ensure_columns(db_path: str) -> None:
    with sqlite3.connect(db_path) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(news_items)")}
        for name, definition in {
            "resolved_url": "TEXT",
            "extraction_status": "TEXT",
            "extraction_method": "TEXT",
            "content_length": "INTEGER",
            "extraction_error": "TEXT",
            "extraction_attempts": "INTEGER NOT NULL DEFAULT 0",
            "last_extraction_at_utc": "TEXT",
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


def _download(original_url: str):
    """Resolve and download one article; safe to run in a worker thread."""
    resolved = None
    try:
        resolved = decode_url(original_url)
        return resolved, extract(resolved), None
    except Exception as error:
        return resolved, None, error


def crawl(
    db_path: str,
    limit: int,
    delay: float,
    max_attempts: int = 5,
    hybrid_extractor=None,
    workers: int = 1,
) -> tuple[int, int, int]:
    ensure_columns(db_path)
    with sqlite3.connect(db_path) as connection:
        rows = connection.execute(
            """SELECT id,url,title,summary,source FROM news_items
               WHERE (content IS NULL OR length(content) < 200)
                 AND tickers_json != '[]'
                 AND (extraction_status IS NULL OR extraction_status NOT IN ('success','skipped'))
                 AND COALESCE(extraction_attempts,0) < ?
               ORDER BY id LIMIT ?""", (max_attempts, limit)
        ).fetchall()
    success = failed = skipped = 0
    downloadable = []
    for row in rows:
        news_id, original_url, *_ = row
        if not original_url:
            with sqlite3.connect(db_path) as connection:
                connection.execute(
                    """UPDATE news_items SET extraction_status='skipped',
                       extraction_error='missing article URL',
                       last_extraction_at_utc=? WHERE id=?""",
                    (datetime.now(timezone.utc).isoformat(), news_id),
                )
            skipped += 1
            continue
        downloadable.append(row)

    futures: dict[Future, tuple] = {}
    with ThreadPoolExecutor(max_workers=max(1, workers), thread_name_prefix="article") as executor:
        for index, row in enumerate(downloadable):
            futures[executor.submit(_download, row[1])] = row
            # Stagger request starts to avoid sending a burst to one publisher.
            if delay > 0 and index < len(downloadable) - 1:
                time.sleep(delay)

        for future in as_completed(futures):
            news_id, original_url, original_title, summary, source = futures[future]
            resolved, item, error = future.result()
            if error is None:
                assert item is not None
                news_item = NewsItem(
                    title=item.title or original_title or "", summary=summary or "",
                    source=source or "", url=resolved, content=item.text,
                )
                extract_item_tickers(news_item, hybrid_extractor)
                with sqlite3.connect(db_path) as connection:
                    connection.execute(
                        """UPDATE news_items SET title=?,content=?,author=?,image_url=?,
                           resolved_url=?, extraction_status='success', extraction_method=?,
                           content_length=?, extraction_error=NULL,
                           extraction_attempts=COALESCE(extraction_attempts,0)+1,
                           last_extraction_at_utc=?,tickers_json=?,rulebased_tickers_json=?,
                           ner_tickers_json=?,ticker_extraction_strategy=? WHERE id=?""",
                        (news_item.title, item.text, ", ".join(item.authors) if item.authors else None,
                         item.top_image or None, resolved, "google_decoder+newspaper4k",
                         len(item.text), datetime.now(timezone.utc).isoformat(),
                         json.dumps(news_item.tickers, ensure_ascii=False),
                         json.dumps(news_item.rulebased_tickers, ensure_ascii=False),
                         json.dumps(news_item.ner_tickers, ensure_ascii=False),
                         news_item.ticker_extraction_strategy, news_id),
                    )
                success += 1
                print(f"{news_id}: success chars={len(item.text)} {resolved}")
            else:
                with sqlite3.connect(db_path) as connection:
                    connection.execute(
                        """UPDATE news_items SET resolved_url=?, extraction_status='failed',
                           extraction_method='google_decoder+newspaper4k', content_length=0,
                           extraction_error=?, extraction_attempts=COALESCE(extraction_attempts,0)+1,
                           last_extraction_at_utc=? WHERE id=?""",
                        (resolved, str(error)[:500], datetime.now(timezone.utc).isoformat(), news_id),
                    )
                failed += 1
                print(f"{news_id}: failed {type(error).__name__}: {error}")
    return success, failed, skipped


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="news_system.db")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--delay", type=float, default=1.0)
    parser.add_argument("--workers", type=int, default=1,
                        help="Concurrent article downloads (requests remain staggered by --delay)")
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--ner-model", default="models/ticker_ner_crf.joblib")
    parser.add_argument("--watch", action="store_true",
                        help="Keep processing bounded batches until Ctrl+C")
    parser.add_argument("--interval-minutes", type=int, default=15)
    args = parser.parse_args()
    if args.limit < 1 or args.workers < 1 or args.max_attempts < 1 or args.interval_minutes < 1:
        parser.error("--limit, --workers, --max-attempts and --interval-minutes must be positive")
    if args.delay < 0:
        parser.error("--delay cannot be negative")
    hybrid_extractor = None
    if args.ner_model:
        from pathlib import Path
        model_path = Path(args.ner_model)
        if model_path.is_file():
            from .hybrid_ticker_extractor import HybridTickerExtractor
            hybrid_extractor = HybridTickerExtractor(model_path)
        else:
            print(f"[WARN] NER model not found; using rule-based extraction: {model_path}")
    try:
        while True:
            result = crawl(
                args.db, args.limit, args.delay, args.max_attempts,
                hybrid_extractor, args.workers,
            )
            print(f"completed success={result[0]} failed={result[1]} skipped={result[2]}")
            if not args.watch:
                break
            print(f"Next historical enrichment batch in {args.interval_minutes} minute(s).")
            time.sleep(args.interval_minutes * 60)
    except KeyboardInterrupt:
        print("Historical enrichment stopped.")


if __name__ == "__main__":
    main()
