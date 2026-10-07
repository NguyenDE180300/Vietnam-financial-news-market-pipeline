from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class NewsItem:
    title: str
    summary: str = ""
    source: str = ""
    url: str = ""
    published_at: str | None = None
    category: str = "DOANH NGHIỆP"
    tickers: list[str] = field(default_factory=list)
    rulebased_tickers: list[str] = field(default_factory=list)
    ner_tickers: list[str] = field(default_factory=list)
    ticker_extraction_strategy: str = "rulebased"
    event_time: str | None = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    published_precision: str = "unknown"
    content: str = ""
    author: str | None = None
    image_url: str | None = None


@dataclass
class MarketBar:
    ticker: str
    bar_time: str
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: float | None
    interval: str = "1d"
    source: str = ""
