"""Point-in-time news/market join for the MVP."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime


def _time(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def build_event_market_rows(db_path: str, horizons: tuple[int, ...] = (1, 3, 5)) -> list[dict]:
    """Join each event to the latest known bar and subsequent sessions.

    The event bar is selected with ``bar_time <= event_time``. Missing bars are
    retained as nulls so data quality is visible rather than silently filled.
    """
    with sqlite3.connect(db_path) as connection:
        news = connection.execute(
            "SELECT id,tickers_json,event_time FROM news_items WHERE event_time IS NOT NULL"
        ).fetchall()
        bars = connection.execute(
            "SELECT ticker,bar_time,close FROM market_bars ORDER BY ticker,bar_time"
        ).fetchall()

    by_ticker: dict[str, list[tuple[datetime, float | None]]] = {}
    for ticker, bar_time, close in bars:
        by_ticker.setdefault(ticker, []).append((_time(bar_time), close))

    output = []
    for news_id, tickers_json, event_time in news:
        event_dt = _time(event_time)
        for ticker in json.loads(tickers_json or "[]"):
            ticker_bars = by_ticker.get(ticker, [])
            anchor = [index for index, (bar_dt, _) in enumerate(ticker_bars) if bar_dt <= event_dt]
            if not anchor:
                continue
            anchor_index = anchor[-1]
            anchor_close = ticker_bars[anchor_index][1]
            row = {"news_id": news_id, "ticker": ticker, "event_time": event_time,
                   "price_at_event": anchor_close, "join_quality": "point_in_time"}
            for horizon in horizons:
                target = anchor_index + horizon
                future_close = ticker_bars[target][1] if target < len(ticker_bars) else None
                # ``horizon`` means trading sessions, not calendar days.
                row[f"return_{horizon}session"] = (
                    (future_close / anchor_close - 1) if anchor_close and future_close else None
                )
            output.append(row)
    return output
