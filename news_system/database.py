from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

from .models import MarketBar, NewsItem


SCHEMA = """
CREATE TABLE IF NOT EXISTS news_items (
 id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, summary TEXT,
 source TEXT, url TEXT UNIQUE, published_at TEXT, event_time TEXT,
 category TEXT, content_hash TEXT NOT NULL, tickers_json TEXT NOT NULL,
 published_precision TEXT NOT NULL DEFAULT 'unknown', observed_at TEXT,
 created_at TEXT NOT NULL, content TEXT, author TEXT, image_url TEXT
);
CREATE TABLE IF NOT EXISTS market_bars (
 ticker TEXT NOT NULL, bar_time TEXT NOT NULL, interval TEXT NOT NULL,
 open REAL, high REAL, low REAL, close REAL, volume REAL, source TEXT,
 PRIMARY KEY (ticker, bar_time, interval, source)
);
CREATE TABLE IF NOT EXISTS news_market_impacts (
 news_id INTEGER NOT NULL, ticker TEXT NOT NULL, horizon_sessions INTEGER NOT NULL,
 event_time TEXT NOT NULL, anchor_date TEXT NOT NULL, target_date TEXT NOT NULL,
 anchor_close REAL NOT NULL, target_close REAL NOT NULL,
 raw_return REAL NOT NULL, benchmark_return REAL, abnormal_return REAL,
 impact_label TEXT NOT NULL, join_quality TEXT NOT NULL,
 calculated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY (news_id, ticker, horizon_sessions)
);
CREATE TABLE IF NOT EXISTS news_market_event_windows (
 news_id INTEGER NOT NULL, ticker TEXT NOT NULL, relative_session INTEGER NOT NULL,
 event_time TEXT NOT NULL, t0_date TEXT NOT NULL, session_date TEXT NOT NULL,
 close REAL NOT NULL, session_return REAL,
 benchmark_session_return REAL, abnormal_session_return REAL,
 impact_label TEXT, join_quality TEXT NOT NULL,
 calculated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY (news_id, ticker, relative_session)
);
CREATE INDEX IF NOT EXISTS idx_news_event_time ON news_items(event_time);
CREATE INDEX IF NOT EXISTS idx_market_ticker_time ON market_bars(ticker, bar_time);
CREATE INDEX IF NOT EXISTS idx_impact_news ON news_market_impacts(news_id);
CREATE INDEX IF NOT EXISTS idx_impact_ticker ON news_market_impacts(ticker);
CREATE INDEX IF NOT EXISTS idx_event_window_news ON news_market_event_windows(news_id);
CREATE INDEX IF NOT EXISTS idx_event_window_ticker ON news_market_event_windows(ticker);
"""


class NewsDatabase:
    def __init__(self, path: str | Path = "news_system.db"):
        self.path = str(path)
        with sqlite3.connect(self.path) as connection:
            connection.executescript(SCHEMA)
            columns = {row[1] for row in connection.execute("PRAGMA table_info(news_items)")}
            if "published_precision" not in columns:
                connection.execute("ALTER TABLE news_items ADD COLUMN published_precision TEXT NOT NULL DEFAULT 'unknown'")
            if "observed_at" not in columns:
                connection.execute("ALTER TABLE news_items ADD COLUMN observed_at TEXT")
            for column in ("content", "author", "image_url"):
                if column not in columns:
                    connection.execute(f"ALTER TABLE news_items ADD COLUMN {column} TEXT")
            additions = {
                "rulebased_tickers_json": "TEXT NOT NULL DEFAULT '[]'",
                "ner_tickers_json": "TEXT NOT NULL DEFAULT '[]'",
                "ticker_extraction_strategy": "TEXT NOT NULL DEFAULT 'rulebased'",
            }
            for column, definition in additions.items():
                if column not in columns:
                    connection.execute(f"ALTER TABLE news_items ADD COLUMN {column} {definition}")

    def add_news(self, item: NewsItem) -> int | None:
        digest = hashlib.sha256(f"{item.title}|{item.summary}".encode()).hexdigest()
        with sqlite3.connect(self.path) as connection:
            if item.url:
                existing = connection.execute(
                    """SELECT id,tickers_json,rulebased_tickers_json,ner_tickers_json,
                       ticker_extraction_strategy FROM news_items WHERE url=?""", (item.url,)
                ).fetchone()
            else:
                existing = connection.execute(
                    """SELECT id,tickers_json,rulebased_tickers_json,ner_tickers_json,
                       ticker_extraction_strategy FROM news_items WHERE content_hash=?""", (digest,)
                ).fetchone()
            if existing:
                merged_tickers = list(dict.fromkeys(json.loads(existing[1] or "[]") + item.tickers))
                merged_rulebased = list(dict.fromkeys(
                    json.loads(existing[2] or "[]") + item.rulebased_tickers
                ))
                merged_ner = list(dict.fromkeys(json.loads(existing[3] or "[]") + item.ner_tickers))
                strategy = (
                    item.ticker_extraction_strategy
                    if item.ticker_extraction_strategy != "rulebased"
                    else existing[4]
                )
                connection.execute(
                    """UPDATE news_items SET tickers_json=?, rulebased_tickers_json=?,
                       ner_tickers_json=?, ticker_extraction_strategy=? WHERE id=?""",
                    (json.dumps(merged_tickers, ensure_ascii=False),
                     json.dumps(merged_rulebased, ensure_ascii=False),
                     json.dumps(merged_ner, ensure_ascii=False), strategy, existing[0]),
                )
                return None
            cursor = connection.execute(
                """INSERT OR IGNORE INTO news_items
                (title,summary,source,url,published_at,event_time,category,content_hash,tickers_json,
                 published_precision,observed_at,created_at,content,author,image_url,
                 rulebased_tickers_json,ner_tickers_json,ticker_extraction_strategy)
                VALUES (?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,?,?,?,?,?,?,?)""",
                (item.title, item.summary, item.source, item.url or None, item.published_at,
                 item.event_time or item.published_at, item.category,
                 hashlib.sha256(f"{item.title}|{item.content or item.summary}".encode()).hexdigest(),
                 json.dumps(item.tickers, ensure_ascii=False), item.published_precision, item.created_at,
                 item.content or None, item.author, item.image_url,
                 json.dumps(item.rulebased_tickers, ensure_ascii=False),
                 json.dumps(item.ner_tickers, ensure_ascii=False),
                 item.ticker_extraction_strategy),
            )
            return cursor.lastrowid or None

    def add_market_bar(self, bar: MarketBar) -> None:
        self.add_market_bars([bar])

    def add_market_bars(self, bars: list[MarketBar]) -> None:
        if not bars:
            return
        with sqlite3.connect(self.path) as connection:
            connection.executemany(
                """INSERT OR REPLACE INTO market_bars
                (ticker,bar_time,interval,open,high,low,close,volume,source)
                VALUES (?,?,?,?,?,?,?,?,?)""",
                [
                    (bar.ticker, bar.bar_time, bar.interval, bar.open, bar.high,
                     bar.low, bar.close, bar.volume, bar.source)
                    for bar in bars
                ],
            )

    def list_news(self, limit: int = 50) -> list[tuple]:
        with sqlite3.connect(self.path) as connection:
            return connection.execute(
                "SELECT id,title,category,tickers_json,source,url,event_time FROM news_items ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()

    def search_ticker(self, ticker: str, limit: int = 50) -> list[tuple]:
        with sqlite3.connect(self.path) as connection:
            return connection.execute(
                """SELECT id,title,category,tickers_json,source,url,published_at
                   FROM news_items WHERE EXISTS (
                     SELECT 1 FROM json_each(news_items.tickers_json) WHERE value=?
                   ) ORDER BY published_at DESC LIMIT ?""",
                (ticker.upper(), limit),
            ).fetchall()

    def stats(self) -> dict:
        with sqlite3.connect(self.path) as connection:
            total = connection.execute("SELECT COUNT(*) FROM news_items").fetchone()[0]
            by_source = connection.execute(
                "SELECT source,COUNT(*) FROM news_items GROUP BY source ORDER BY COUNT(*) DESC"
            ).fetchall()
            by_category = connection.execute(
                "SELECT category,COUNT(*) FROM news_items GROUP BY category ORDER BY COUNT(*) DESC"
            ).fetchall()
        return {"total": total, "by_source": by_source, "by_category": by_category}

    def news_tickers(self) -> list[str]:
        """Return unique extracted VN tickers currently present in saved news."""
        with sqlite3.connect(self.path) as connection:
            rows = connection.execute("SELECT tickers_json FROM news_items").fetchall()
        tickers: set[str] = set()
        for (payload,) in rows:
            tickers.update(json.loads(payload or "[]"))
        return sorted(tickers)
