from __future__ import annotations

import argparse
import csv
import time

from .database import NewsDatabase
from .article_extractor import extract_article
from .market_collector import download_daily
from .rss_collector import DEFAULT_FEEDS, collect_feed


def collect_news(database: NewsDatabase, feeds: dict[str, str] = DEFAULT_FEEDS,
                 full_text: bool = False, article_limit: int = 0,
                 hybrid_extractor=None, only_with_ticker: bool = False) -> int:
    inserted = 0
    fetched_articles = 0
    for source, url in feeds.items():
        try:
            items = collect_feed(url, source)
        except Exception as error:
            print(f"[WARN] {source}: {error}")
            continue
        for item in items:
            should_fetch_article = (full_text and item.url and
                                    (article_limit <= 0 or fetched_articles < article_limit))
            if should_fetch_article:
                try:
                    article = extract_article(item.url)
                    item.content = article.content
                    item.author = article.author
                    item.image_url = article.image_url or item.image_url
                    if article.title:
                        item.title = article.title
                    fetched_articles += 1
                    time.sleep(0.5)
                except Exception as error:
                    print(f"[WARN] article {item.url}: {error}")
            text = f"{item.title}\n\n{item.content or item.summary}".strip()
            if hybrid_extractor is not None:
                result = hybrid_extractor.extract_with_details(text)
                item.rulebased_tickers = result.rulebased_tickers
                item.ner_tickers = result.ner_tickers
                item.tickers = result.merged_tickers
                item.ticker_extraction_strategy = "rulebased+crf_ner"
            else:
                from .ticker_extractor import TickerExtractor, VN30_TICKERS
                item.rulebased_tickers = [
                    ticker for ticker in TickerExtractor().extract(text)
                    if ticker in set(VN30_TICKERS)
                ]
                item.ner_tickers = []
                item.tickers = item.rulebased_tickers
                item.ticker_extraction_strategy = "rulebased"
            if only_with_ticker and not item.tickers:
                continue
            inserted += bool(database.add_news(item))
        print(f"{source}: fetched={len(items)}")
    return inserted


def collect_market(database: NewsDatabase, tickers: list[str], period: str,
                   start: str | None = None, end: str | None = None) -> None:
    for ticker in tickers:
        bars = download_daily(ticker, period, start=start, end=end)
        database.add_market_bars(bars)
        print(f"{ticker}: bars={len(bars)}")


def sync_market_from_news(database: NewsDatabase, period: str = "1mo") -> None:
    """Download market bars for tickers found in the saved news table.

    News timestamps retain their source offset (normally +07:00), while Yahoo
    bars are stored in UTC. This makes event/market comparisons unambiguous.
    """
    tickers = database.news_tickers()
    if not tickers:
        print("No extracted tickers found; collect news first.")
        return
    print(f"Syncing market data for {len(tickers)} ticker(s): {', '.join(tickers)}")
    collect_market(database, [f"{ticker}.VN" for ticker in tickers], period)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="news_system.db")
    parser.add_argument("--news", action="store_true")
    parser.add_argument("--full-text", action="store_true", help="Fetch article pages after RSS discovery")
    parser.add_argument("--article-limit", type=int, default=0,
                        help="Limit full-text page fetches (0 means no limit)")
    parser.add_argument("--hybrid-model", metavar="JOBLIB",
                        help="Run rule-based and CRF NER in parallel using this model bundle")
    parser.add_argument("--only-with-ticker", action="store_true",
                        help="Save only articles having a merged VN30 ticker")
    parser.add_argument("--watch", action="store_true", help="Continuously poll RSS feeds")
    parser.add_argument("--interval-minutes", type=int, default=15)
    parser.add_argument("--market", nargs="*", metavar="TICKER")
    parser.add_argument("--sync-market", action="store_true",
                        help="Download market data for tickers already extracted from saved news")
    parser.add_argument("--period", default="1mo")
    parser.add_argument("--start", help="Historical start date YYYY-MM-DD")
    parser.add_argument("--end", help="Historical end date YYYY-MM-DD")
    parser.add_argument("--show", action="store_true", help="Show latest saved news")
    parser.add_argument("--ticker", help="Search saved news by ticker")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--stats", action="store_true", help="Show database counts")
    parser.add_argument("--export", metavar="CSV", help="Export news records to CSV")
    args = parser.parse_args()
    if args.interval_minutes < 1:
        parser.error("--interval-minutes must be at least 1")
    database = NewsDatabase(args.db)
    hybrid_extractor = None
    if args.hybrid_model:
        from .hybrid_ticker_extractor import HybridTickerExtractor
        hybrid_extractor = HybridTickerExtractor(args.hybrid_model)
    if args.watch:
        print(f"Polling RSS every {args.interval_minutes} minute(s). Press Ctrl+C to stop.")
        try:
            while True:
                inserted = collect_news(
                    database, full_text=args.full_text, article_limit=args.article_limit,
                    hybrid_extractor=hybrid_extractor, only_with_ticker=args.only_with_ticker,
                )
                if args.sync_market:
                    sync_market_from_news(database, args.period)
                print(f"new articles inserted={inserted}; next poll in {args.interval_minutes} minute(s)")
                time.sleep(args.interval_minutes * 60)
        except KeyboardInterrupt:
            print("RSS polling stopped.")
    elif args.news:
        inserted = collect_news(
            database, full_text=args.full_text, article_limit=args.article_limit,
            hybrid_extractor=hybrid_extractor, only_with_ticker=args.only_with_ticker,
        )
        print(f"inserted={inserted}")
    if args.market is not None:
        collect_market(database, args.market or ["FPT.VN", "HPG.VN", "VCB.VN"], args.period, args.start, args.end)
    if args.sync_market:
        sync_market_from_news(database, args.period)
    if args.show or args.ticker:
        rows = database.search_ticker(args.ticker, args.limit) if args.ticker else database.list_news(args.limit)
        for news_id, title, category, tickers, source, url, published_at in rows:
            print(f"[{news_id}] {published_at or 'unknown date'} | {category} | {tickers} | {source}")
            print(f"{title}\n{url or ''}\n")
    if args.stats:
        stats = database.stats()
        print(f"Total news: {stats['total']}")
        print("By source:", stats["by_source"])
        print("By category:", stats["by_category"])
    if args.export:
        rows = database.list_news(limit=1_000_000)
        with open(args.export, "w", newline="", encoding="utf-8-sig") as output:
            writer = csv.writer(output)
            writer.writerow(["id", "title", "category", "tickers_json", "source", "url", "published_at"])
            writer.writerows(rows)
        print(f"Exported {len(rows)} news rows to {args.export}")
    if not any((args.news, args.watch, args.market is not None, args.sync_market, args.show, args.ticker, args.stats, args.export)):
        parser.print_help()


if __name__ == "__main__":
    main()
