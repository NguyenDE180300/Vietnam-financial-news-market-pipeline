"""Collect daily market bars into immutable Bronze JSONL.GZ batches."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from datetime import datetime, timezone

from .lake_storage import create_lake_storage
from .market_collector import download_daily, download_vnstock_daily
from .ticker_extractor import VN30_TICKERS


def market_event_id(ticker: str, session_date: str, source: str) -> str:
    identity = f"{ticker}|{session_date}|1d|{source}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def collect_market_batch(
    storage,
    tickers: list[str],
    provider: str,
    start: str | None = None,
    end: str | None = None,
    period: str = "1mo",
) -> tuple[str, int]:
    collected_at = datetime.now(timezone.utc)
    records: list[dict] = []
    for requested in tickers:
        ticker = requested.upper().removesuffix(".VN")
        try:
            if provider == "yahoo":
                bars = download_daily(
                    f"{ticker}.VN", period=period, start=start, end=end
                )
                source = "yahoo_adjusted"
            else:
                if not start or not end:
                    raise ValueError("vnstock requires --start and --end")
                bars = download_vnstock_daily(ticker, start, end)
                source = "vnstock_vci"
            for bar in bars:
                session_date = bar.bar_time[:10]
                records.append({
                    "event_id": market_event_id(ticker, session_date, source),
                    "schema_version": 1,
                    "event_type": "market.daily.collected",
                    "ticker": ticker,
                    "session_date": session_date,
                    "interval": "1d",
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                    "currency": "VND",
                    "price_adjustment": "split_dividend_adjusted" if provider == "yahoo" else "provider_default",
                    "source": source,
                    "collected_at_utc": collected_at.isoformat(),
                    "ingest_date": collected_at.date().isoformat(),
                })
            print(f"{ticker}: fetched={len(bars)} provider={provider}", flush=True)
        except Exception as error:
            print(f"[WARN] market {ticker}: {error}", flush=True)

    if not records:
        raise RuntimeError("No market bars were collected")
    # One provider can occasionally return duplicate rows. Keep Bronze compact;
    # Silver still enforces the authoritative deduplication contract.
    records = list({record["event_id"]: record for record in records}.values())
    payload = "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records)
    compressed = gzip.compress(payload.encode("utf-8"), compresslevel=6)
    stamp = collected_at.strftime("%Y%m%dT%H%M%S%fZ")
    relative_path = (
        f"market/ingest_date={collected_at:%Y-%m-%d}/"
        f"batch_{stamp}.jsonl.gz"
    )
    path = storage.write_bytes("bronze", relative_path, compressed)
    print(f"Market Bronze: records={len(records)} path={path}", flush=True)
    return path, len(records)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=("local", "adls"), default="local")
    parser.add_argument("--root", default="data_lake")
    parser.add_argument("--account-name", help="ADLS account; defaults to AZURE_STORAGE_ACCOUNT")
    parser.add_argument("--provider", choices=("yahoo", "vnstock"), default="yahoo")
    parser.add_argument("--tickers", nargs="+", default=None)
    parser.add_argument("--vn30", action="store_true", help="Collect the VN30 snapshot")
    parser.add_argument("--period", default="1mo", help="Yahoo period when dates are omitted")
    parser.add_argument("--start", help="Inclusive date YYYY-MM-DD")
    parser.add_argument("--end", help="Yahoo end is exclusive; vnstock end follows provider semantics")
    args = parser.parse_args()
    tickers = VN30_TICKERS if args.vn30 else (args.tickers or ["FPT", "HPG", "VCB"])
    storage = create_lake_storage(args.backend, args.root, args.account_name)
    collect_market_batch(
        storage, tickers, args.provider, args.start, args.end, args.period
    )


if __name__ == "__main__":
    main()
