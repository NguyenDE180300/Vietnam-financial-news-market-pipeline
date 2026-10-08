"""Resolve Google News URLs and enrich saved news with publisher articles."""
from __future__ import annotations

import argparse
import json
import sqlite3
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from .models import NewsItem
from .news_ingestion import extract_item_tickers


class RateLimitError(RuntimeError):
    """Transient upstream throttling that must not consume an article retry."""


_GOOGLE_RESOLVER_LOCK = threading.Lock()
_google_next_allowed = 0.0
_google_backoff_level = 0


def _is_rate_limited(error: object) -> bool:
    message = str(error).casefold()
    return "429" in message or "too many requests" in message


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


def decode_url(url: str, resolve_delay: float = 3.0, backoff_seconds: float = 120.0) -> str:
    if not url.startswith("https://news.google.com/"):
        return url
    global _google_next_allowed, _google_backoff_level
    from googlenewsdecoder import gnewsdecoder
    # Google URL resolution is deliberately serialized even though publisher
    # downloads remain concurrent. This prevents six workers from hitting the
    # decoder endpoint at the same instant.
    with _GOOGLE_RESOLVER_LOCK:
        wait_seconds = _google_next_allowed - time.monotonic()
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        try:
            result = gnewsdecoder(url, interval=max(0.5, resolve_delay))
            if not result.get("status"):
                raise RuntimeError(str(result))
        except Exception as error:
            if _is_rate_limited(error):
                cooldown = min(backoff_seconds * (2 ** _google_backoff_level), 900.0)
                _google_backoff_level = min(_google_backoff_level + 1, 3)
                _google_next_allowed = time.monotonic() + cooldown
                raise RateLimitError(f"Google News rate limited; cooldown={cooldown:.0f}s") from error
            _google_next_allowed = time.monotonic() + resolve_delay
            raise
        _google_backoff_level = 0
        _google_next_allowed = time.monotonic() + resolve_delay
        return result["decoded_url"]


def extract(url: str):
    from newspaper import article
    item = article(url)
    if not item.text or len(item.text.strip()) < 200:
        raise RuntimeError(f"content too short ({len(item.text.strip())} chars)")
    return item


def _download(original_url: str, google_resolve_delay: float, rate_limit_backoff: float):
    """Resolve and download one article; safe to run in a worker thread."""
    resolved = None
    try:
        resolved = decode_url(original_url, google_resolve_delay, rate_limit_backoff)
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
    google_resolve_delay: float = 3.0,
    rate_limit_backoff: float = 120.0,
) -> tuple[int, int, int, int]:
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
    success = failed = skipped = rate_limited = 0
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
            futures[
                executor.submit(
                    _download, row[1], google_resolve_delay, rate_limit_backoff
                )
            ] = row
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
            elif isinstance(error, RateLimitError):
                with sqlite3.connect(db_path) as connection:
                    connection.execute(
                        """UPDATE news_items SET resolved_url=?, extraction_status='rate_limited',
                           extraction_method='google_decoder+newspaper4k', extraction_error=?,
                           last_extraction_at_utc=? WHERE id=?""",
                        (resolved, str(error)[:500], datetime.now(timezone.utc).isoformat(), news_id),
                    )
                rate_limited += 1
                print(f"{news_id}: rate_limited {error}")
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
    return success, failed, skipped, rate_limited


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="news_system.db")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--delay", type=float, default=1.0)
    parser.add_argument("--workers", type=int, default=1,
                        help="Concurrent article downloads (requests remain staggered by --delay)")
    parser.add_argument("--google-resolve-delay", type=float, default=3.0,
                        help="Minimum seconds between serialized Google URL resolutions")
    parser.add_argument("--rate-limit-backoff", type=float, default=120.0,
                        help="Initial Google 429 cooldown; doubles up to 15 minutes")
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--ner-model", default="models/ticker_ner_crf.joblib")
    parser.add_argument("--watch", action="store_true",
                        help="Keep processing bounded batches until Ctrl+C")
    parser.add_argument("--interval-minutes", type=int, default=15)
    args = parser.parse_args()
    if args.limit < 1 or args.workers < 1 or args.max_attempts < 1 or args.interval_minutes < 1:
        parser.error("--limit, --workers, --max-attempts and --interval-minutes must be positive")
    if args.delay < 0 or args.google_resolve_delay < 0 or args.rate_limit_backoff < 1:
        parser.error("delays cannot be negative and --rate-limit-backoff must be >= 1")
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
                hybrid_extractor, args.workers, args.google_resolve_delay,
                args.rate_limit_backoff,
            )
            print(
                f"completed success={result[0]} failed={result[1]} "
                f"skipped={result[2]} rate_limited={result[3]}"
            )
            if not args.watch:
                break
            print(f"Next historical enrichment batch in {args.interval_minutes} minute(s).")
            time.sleep(args.interval_minutes * 60)
    except KeyboardInterrupt:
        print("Historical enrichment stopped.")


if __name__ == "__main__":
    main()
