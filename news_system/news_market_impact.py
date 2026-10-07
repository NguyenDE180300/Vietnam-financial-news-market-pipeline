"""Measure post-news stock returns without leaking future market data."""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import time as time_module
from bisect import bisect_left, bisect_right
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from .database import NewsDatabase
from .market_collector import download_vnstock_daily
from .ticker_extractor import VN30_TICKERS


VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
MARKET_CLOSE = time(15, 0)
BENCHMARK = "VNINDEX"


@dataclass
class ImpactRow:
    news_id: int
    ticker: str
    horizon_sessions: int
    event_time: str
    anchor_date: str
    target_date: str
    anchor_close: float
    target_close: float
    raw_return: float
    benchmark_return: float | None
    abnormal_return: float | None
    impact_label: str
    join_quality: str


@dataclass
class EventWindowRow:
    news_id: int
    ticker: str
    relative_session: int
    event_time: str
    t0_date: str
    session_date: str
    close: float
    session_return: float | None
    benchmark_session_return: float | None
    abnormal_session_return: float | None
    impact_label: str | None
    join_quality: str


def _parse_event(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=VN_TZ)
    return parsed.astimezone(VN_TZ)


def _bar_date(value: str) -> date:
    """Yahoo daily timestamps identify a trading date, not an event instant."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).date()


def _label(value: float, threshold: float) -> str:
    if value >= threshold:
        return "positive"
    if value <= -threshold:
        return "negative"
    return "neutral"


def _load_bars(connection: sqlite3.Connection) -> dict[str, list[tuple[date, float]]]:
    sources = connection.execute(
        """SELECT ticker,source FROM market_bars WHERE interval='1d' AND close IS NOT NULL
           GROUP BY ticker,source"""
    ).fetchall()
    preferred = {
        ticker: ("vnstock_vci" if "vnstock_vci" in ticker_sources else sorted(ticker_sources)[0])
        for ticker, ticker_sources in _group_sources(sources).items()
    }
    rows = connection.execute(
        """SELECT ticker,bar_time,close,source FROM market_bars
           WHERE interval='1d' AND close IS NOT NULL ORDER BY ticker,bar_time"""
    ).fetchall()
    output: dict[str, list[tuple[date, float]]] = {}
    for ticker, bar_time, close, source in rows:
        if source == preferred.get(ticker):
            output.setdefault(ticker, []).append((_bar_date(bar_time), float(close)))
    return output


def _group_sources(rows: list[tuple[str, str]]) -> dict[str, set[str]]:
    grouped: dict[str, set[str]] = {}
    for ticker, source in rows:
        grouped.setdefault(ticker, set()).add(source)
    return grouped


def _known_anchor_index(bars: list[tuple[date, float]], event: datetime) -> int | None:
    """Return the latest close that was publicly known when the event occurred."""
    dates = [bar_date for bar_date, _ in bars]
    event_date = event.date()
    position = bisect_right(dates, event_date) - 1
    if position < 0:
        return None
    # A same-day close is not known before the market closes.
    if dates[position] == event_date and event.timetz().replace(tzinfo=None) < MARKET_CLOSE:
        position -= 1
    return position if position >= 0 else None


def _t0_index(bars: list[tuple[date, float]], event: datetime) -> int | None:
    """First trading session capable of reflecting the published news."""
    dates = [bar_date for bar_date, _ in bars]
    if not dates:
        return None
    same_or_next = bisect_left(dates, event.date())
    if same_or_next >= len(dates):
        return None
    is_same_day = dates[same_or_next] == event.date()
    published_before_close = event.timetz().replace(tzinfo=None) < MARKET_CLOSE
    if is_same_day and published_before_close:
        return same_or_next
    next_session = bisect_right(dates, event.date())
    return next_session if next_session < len(dates) else None


def calculate_event_windows(
    db_path: str | Path,
    offsets: tuple[int, ...] = (-5, -3, -2, -1, 0, 1, 2, 3, 5),
    threshold: float = 0.02,
    benchmark: str = BENCHMARK,
    persist: bool = True,
) -> tuple[list[EventWindowRow], dict[str, int]]:
    """Build per-session AR rows relative to the event's reaction session T0."""
    NewsDatabase(db_path)
    with sqlite3.connect(db_path) as connection:
        news_rows = connection.execute(
            """SELECT id,tickers_json,event_time,published_precision FROM news_items
               WHERE event_time IS NOT NULL AND tickers_json != '[]' ORDER BY id"""
        ).fetchall()
        bars_by_ticker = _load_bars(connection)

    benchmark_bars = bars_by_ticker.get(benchmark, [])
    benchmark_daily_returns = {
        benchmark_bars[index][0]: benchmark_bars[index][1] / benchmark_bars[index - 1][1] - 1
        for index in range(1, len(benchmark_bars))
        if benchmark_bars[index - 1][1]
    }
    output: list[EventWindowRow] = []
    skipped_no_t0 = skipped_outside_history = 0
    for news_id, tickers_json, event_time, precision in news_rows:
        try:
            event = _parse_event(event_time)
        except (TypeError, ValueError):
            continue
        for ticker in json.loads(tickers_json or "[]"):
            bars = bars_by_ticker.get(ticker, [])
            t0_index = _t0_index(bars, event)
            if t0_index is None:
                skipped_no_t0 += 1
                continue
            t0_date = bars[t0_index][0]
            for offset in offsets:
                index = t0_index + offset
                if index < 0 or index >= len(bars):
                    skipped_outside_history += 1
                    continue
                session_date, close = bars[index]
                session_return = (
                    close / bars[index - 1][1] - 1
                    if index > 0 and bars[index - 1][1] else None
                )
                benchmark_return = benchmark_daily_returns.get(session_date)
                abnormal_return = (
                    session_return - benchmark_return
                    if session_return is not None and benchmark_return is not None else None
                )
                value = abnormal_return if abnormal_return is not None else session_return
                quality = "exact_abnormal_return" if abnormal_return is not None else "raw_return_no_benchmark"
                if precision != "timestamp":
                    quality += "|imprecise_event_time"
                output.append(EventWindowRow(
                    news_id=int(news_id), ticker=ticker, relative_session=offset,
                    event_time=event_time, t0_date=t0_date.isoformat(),
                    session_date=session_date.isoformat(), close=close,
                    session_return=session_return,
                    benchmark_session_return=benchmark_return,
                    abnormal_session_return=abnormal_return,
                    impact_label=_label(value, threshold) if value is not None else None,
                    join_quality=quality,
                ))

    if persist:
        with sqlite3.connect(db_path) as connection:
            placeholders = ",".join("?" for _ in offsets)
            connection.execute(
                f"DELETE FROM news_market_event_windows WHERE relative_session IN ({placeholders})",
                offsets,
            )
            connection.executemany(
                """INSERT OR REPLACE INTO news_market_event_windows
                   (news_id,ticker,relative_session,event_time,t0_date,session_date,
                    close,session_return,benchmark_session_return,abnormal_session_return,
                    impact_label,join_quality,calculated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)""",
                [(
                    row.news_id, row.ticker, row.relative_session, row.event_time,
                    row.t0_date, row.session_date, row.close, row.session_return,
                    row.benchmark_session_return, row.abnormal_session_return,
                    row.impact_label, row.join_quality,
                ) for row in output],
            )
    report = {
        "event_window_rows": len(output), "offsets": len(offsets),
        "skipped_no_t0": skipped_no_t0,
        "skipped_outside_history": skipped_outside_history,
    }
    return output, report


def calculate_impacts(db_path: str | Path, horizons: tuple[int, ...] = (1, 3, 5),
                      threshold: float = 0.02, benchmark: str = BENCHMARK,
                      persist: bool = True) -> tuple[list[ImpactRow], dict[str, int]]:
    NewsDatabase(db_path)
    with sqlite3.connect(db_path) as connection:
        news_rows = connection.execute(
            """SELECT id,tickers_json,event_time,published_precision FROM news_items
               WHERE event_time IS NOT NULL AND tickers_json != '[]' ORDER BY id"""
        ).fetchall()
        bars_by_ticker = _load_bars(connection)

    benchmark_bars = bars_by_ticker.get(benchmark, [])
    benchmark_by_date = dict(benchmark_bars)
    output: list[ImpactRow] = []
    skipped_no_anchor = skipped_no_future = 0
    for news_id, tickers_json, event_time, precision in news_rows:
        try:
            event = _parse_event(event_time)
        except (TypeError, ValueError):
            continue
        for ticker in json.loads(tickers_json or "[]"):
            bars = bars_by_ticker.get(ticker, [])
            anchor_index = _known_anchor_index(bars, event)
            if anchor_index is None:
                skipped_no_anchor += 1
                continue
            anchor_date, anchor_close = bars[anchor_index]
            for horizon in horizons:
                target_index = anchor_index + horizon
                if target_index >= len(bars):
                    skipped_no_future += 1
                    continue
                target_date, target_close = bars[target_index]
                raw_return = target_close / anchor_close - 1
                benchmark_anchor = benchmark_by_date.get(anchor_date)
                benchmark_target = benchmark_by_date.get(target_date)
                benchmark_return = (
                    benchmark_target / benchmark_anchor - 1
                    if benchmark_anchor and benchmark_target else None
                )
                abnormal_return = (
                    raw_return - benchmark_return if benchmark_return is not None else None
                )
                impact_value = abnormal_return if abnormal_return is not None else raw_return
                quality = "exact_abnormal_return" if abnormal_return is not None else "raw_return_no_benchmark"
                if precision != "timestamp":
                    quality += "|imprecise_event_time"
                output.append(ImpactRow(
                    news_id=int(news_id), ticker=ticker, horizon_sessions=horizon,
                    event_time=event_time, anchor_date=anchor_date.isoformat(),
                    target_date=target_date.isoformat(), anchor_close=anchor_close,
                    target_close=target_close, raw_return=raw_return,
                    benchmark_return=benchmark_return, abnormal_return=abnormal_return,
                    impact_label=_label(impact_value, threshold), join_quality=quality,
                ))

    if persist:
        with sqlite3.connect(db_path) as connection:
            placeholders = ",".join("?" for _ in horizons)
            connection.execute(
                f"DELETE FROM news_market_impacts WHERE horizon_sessions IN ({placeholders})",
                horizons,
            )
            connection.executemany(
                """INSERT OR REPLACE INTO news_market_impacts
                   (news_id,ticker,horizon_sessions,event_time,anchor_date,target_date,
                    anchor_close,target_close,raw_return,benchmark_return,abnormal_return,
                    impact_label,join_quality,calculated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)""",
                [(
                    row.news_id, row.ticker, row.horizon_sessions, row.event_time,
                    row.anchor_date, row.target_date, row.anchor_close, row.target_close,
                    row.raw_return, row.benchmark_return, row.abnormal_return,
                    row.impact_label, row.join_quality,
                ) for row in output],
            )
    report = {
        "news_with_ticker": len(news_rows), "impact_rows": len(output),
        "skipped_no_anchor": skipped_no_anchor, "skipped_no_future": skipped_no_future,
        "benchmark_available": int(bool(benchmark_bars)),
    }
    return output, report


def sync_market_for_news(db_path: str | Path, start: str | None = None,
                         end: str | None = None, benchmark: str = BENCHMARK,
                         delay: float = 3.2) -> dict[str, int]:
    database = NewsDatabase(db_path)
    with sqlite3.connect(db_path) as connection:
        minimum, maximum = connection.execute(
            "SELECT MIN(event_time),MAX(event_time) FROM news_items WHERE event_time IS NOT NULL"
        ).fetchone()
        payloads = connection.execute(
            "SELECT tickers_json FROM news_items WHERE tickers_json != '[]'"
        ).fetchall()
    if not minimum or not maximum:
        return {"tickers": 0, "bars": 0}
    start_date = start or (_parse_event(minimum).date() - timedelta(days=10)).isoformat()
    # Yahoo's end is exclusive, hence the safety buffer.
    end_date = end or (_parse_event(maximum).date() + timedelta(days=10)).isoformat()
    mentioned = {
        ticker for (payload,) in payloads for ticker in json.loads(payload or "[]")
        if ticker in set(VN30_TICKERS)
    }
    symbols = sorted(mentioned) + [benchmark]
    with sqlite3.connect(db_path) as connection:
        coverage_rows = connection.execute(
            """SELECT ticker,MIN(bar_time),MAX(bar_time),COUNT(*) FROM market_bars
               WHERE source='vnstock_vci' GROUP BY ticker"""
        ).fetchall()
    required_start = date.fromisoformat(start_date)
    required_through = _parse_event(maximum).date()
    completed = {
        ticker for ticker, first, last, count in coverage_rows
        if count > 100 and _bar_date(first) <= required_start and _bar_date(last) >= required_through
    }
    total_bars = 0
    for symbol in symbols:
        if symbol in completed:
            print(f"{symbol}: cached")
            continue
        try:
            bars = download_vnstock_daily(symbol, start_date, end_date)
            database.add_market_bars(bars)
            total_bars += len(bars)
            print(f"{symbol}: bars={len(bars)}")
        except Exception as error:
            print(f"[WARN] {symbol}: {error}")
        time_module.sleep(max(0.0, delay))
    return {"tickers": len(symbols), "bars": total_bars}


def export_csv(rows: list[ImpactRow], output_path: str | Path) -> None:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(ImpactRow.__annotations__))
        writer.writeheader()
        writer.writerows(asdict(row) for row in rows)


def export_event_windows_csv(rows: list[EventWindowRow], output_path: str | Path) -> None:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(EventWindowRow.__annotations__))
        writer.writeheader()
        writer.writerows(asdict(row) for row in rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="news_system.db")
    parser.add_argument("--sync-market", action="store_true")
    parser.add_argument("--start", help="Market history start YYYY-MM-DD")
    parser.add_argument("--end", help="Market history end YYYY-MM-DD (exclusive)")
    parser.add_argument("--request-delay", type=float, default=3.2,
                        help="Delay between vnstock requests to respect community rate limits")
    parser.add_argument("--horizons", nargs="+", type=int, default=[1, 3, 5])
    parser.add_argument("--threshold", type=float, default=0.02)
    parser.add_argument("--output", default="data/news_market_impacts.csv")
    parser.add_argument("--event-offsets", nargs="+", type=int,
                        default=[-5, -3, -2, -1, 0, 1, 2, 3, 5])
    parser.add_argument("--window-output", default="data/news_market_event_windows.csv")
    args = parser.parse_args()
    if args.sync_market:
        print(json.dumps(sync_market_for_news(
            args.db, args.start, args.end, delay=args.request_delay
        ), ensure_ascii=False))
    rows, report = calculate_impacts(
        args.db, tuple(args.horizons), args.threshold, persist=True,
    )
    export_csv(rows, args.output)
    window_rows, window_report = calculate_event_windows(
        args.db, tuple(args.event_offsets), args.threshold, persist=True,
    )
    export_event_windows_csv(window_rows, args.window_output)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(json.dumps(window_report, ensure_ascii=False, indent=2))
    print(f"Saved: {args.output}")
    print(f"Saved: {args.window_output}")


if __name__ == "__main__":
    main()
