"""Historical news backfill through Google News RSS.

This is intentionally separate from the live RSS collector. Google News RSS
is useful for research backfill but is not guaranteed to be a complete archive.
Use --dry-run first; network crawling requires --run explicitly.
"""
from __future__ import annotations

import argparse
from datetime import date
import email.utils
from itertools import islice
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

from .categorizer import categorize
from .cleaner import clean_text
from .database import NewsDatabase
from .models import NewsItem
from .ticker_extractor import TickerExtractor
from .ticker_extractor import VN30_TICKERS


DEFAULT_TICKERS = ["ACB", "BID", "CTG", "FPT", "HPG", "MBB", "MWG", "SSI", "TCB", "VCB", "VHM", "VIC", "VNM", "VPB"]


def month_ranges(start: date, end: date):
    """Yield [start, end) month windows."""
    current = date(start.year, start.month, 1)
    while current < end:
        next_month = date(current.year + (current.month == 12), 1 if current.month == 12 else current.month + 1, 1)
        yield max(current, start), min(next_month, end)
        current = next_month


def query_url(ticker: str, start: date, end: date) -> str:
    query = f"{ticker} after:{start.isoformat()} before:{end.isoformat()}"
    params = {"q": query, "hl": "vi", "gl": "VN", "ceid": "VN:vi"}
    return "https://news.google.com/rss/search?" + urllib.parse.urlencode(params)


def parse_google_rss(payload: bytes, ticker: str, extractor: TickerExtractor | None = None) -> list[NewsItem]:
    extractor = extractor or TickerExtractor()
    root = ET.fromstring(payload)
    items = []
    for element in root.findall(".//item"):
        title = clean_text(element.findtext("title", ""))
        link = clean_text(element.findtext("link", ""))
        summary = clean_text(element.findtext("description", ""))
        publisher = clean_text(element.findtext("source", "")) or "Google News RSS"
        published_raw = clean_text(element.findtext("pubDate", ""))
        if not title or not link:
            continue
        precision = "unknown"
        try:
            published_dt = email.utils.parsedate_to_datetime(published_raw) if published_raw else None
            published = published_dt.isoformat() if published_dt else None
            if published_dt:
                precision = "timestamp"
        except (TypeError, ValueError):
            published = published_raw or None
        text = f"{title} {summary}"
        tickers = extractor.extract(text)
        items.append(NewsItem(
            title=title, summary=summary, source=publisher, url=link,
            published_at=published, event_time=published,
            published_precision=precision,
            category=categorize(title, summary), tickers=tickers,
            rulebased_tickers=tickers,
        ))
    return items


def backfill(tickers: list[str], start: date, end: date, db_path: str,
             delay: float = 1.0, limit: int | None = None, run: bool = False,
             offset: int = 0) -> int:
    windows = list(month_ranges(start, end))
    jobs = [(ticker.upper(), window_start, window_end) for ticker in tickers for window_start, window_end in windows]
    jobs = jobs[offset:]
    if not run:
        print(f"DRY RUN: {len(jobs)} queries ({len(tickers)} tickers × {len(windows)} month windows)")
        for ticker, window_start, window_end in jobs[:5]:
            print(query_url(ticker, window_start, window_end))
        return 0

    database = NewsDatabase(db_path)
    inserted = 0
    completed = 0
    for ticker, window_start, window_end in jobs:
        if limit is not None and completed >= limit:
            break
        url = query_url(ticker, window_start, window_end)
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "news-system-historical/0.1"})
            with urllib.request.urlopen(request, timeout=30) as response:
                items = parse_google_rss(response.read(), ticker)
            for item in items:
                inserted += bool(database.add_news(item))
            print(f"{ticker} {window_start}..{window_end}: fetched={len(items)}")
        except Exception as error:
            print(f"[WARN] {ticker} {window_start}..{window_end}: {error}")
        completed += 1
        time.sleep(max(0.0, delay))
    print(f"Historical backfill complete: inserted={inserted}, queries={completed}")
    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", nargs="+", default=None)
    parser.add_argument("--vn30", action="store_true", help="Use the configured VN30 snapshot")
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--db", default="news_system.db")
    parser.add_argument("--delay", type=float, default=1.0)
    parser.add_argument("--limit", type=int, help="Maximum number of query windows")
    parser.add_argument("--offset", type=int, default=0,
                        help="Skip query windows for resumable batches")
    parser.add_argument("--run", action="store_true", help="Actually make network requests")
    args = parser.parse_args()
    if args.end <= args.start:
        parser.error("--end must be after --start")
    tickers = VN30_TICKERS if args.vn30 else (args.tickers or DEFAULT_TICKERS)
    backfill(tickers, args.start, args.end, args.db, args.delay, args.limit, args.run, args.offset)


if __name__ == "__main__":
    main()
