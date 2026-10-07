from __future__ import annotations

import email.utils
import urllib.request
import xml.etree.ElementTree as ET

from .categorizer import categorize
from .cleaner import clean_text
from .models import NewsItem
from .ticker_extractor import TickerExtractor


DEFAULT_FEEDS = {
    "cafef_stock": "https://cafef.vn/thi-truong-chung-khoan.rss",
    "cafef_company": "https://cafef.vn/doanh-nghiep.rss",
    "cafef_finance": "https://cafef.vn/tai-chinh-ngan-hang.rss",
    "vnexpress_business": "https://vnexpress.net/rss/kinh-doanh.rss",
    "vneconomy_stock": "https://vneconomy.vn/chung-khoan.rss",
}


def _first(element: ET.Element, names: tuple[str, ...]) -> str:
    for name in names:
        child = element.find(name)
        if child is not None and child.text:
            return child.text
    return ""


def parse_rss(xml: bytes, source: str, extractor: TickerExtractor | None = None) -> list[NewsItem]:
    extractor = extractor or TickerExtractor()
    root = ET.fromstring(xml)
    items = []
    for element in root.findall(".//item"):
        title = clean_text(_first(element, ("title",)))
        summary = clean_text(_first(element, ("description", "summary", "content")))
        url = clean_text(_first(element, ("link",)))
        published_raw = clean_text(_first(element, ("pubDate", "published", "date"))) or None
        published = published_raw
        if published_raw:
            try:
                from email.utils import parsedate_to_datetime
                published = parsedate_to_datetime(published_raw).isoformat()
            except (TypeError, ValueError, OverflowError):
                pass
        if not title:
            continue
        tickers = extractor.extract(f"{title} {summary}")
        items.append(NewsItem(
            title=title, summary=summary, source=source, url=url,
            published_at=published, event_time=published,
            published_precision="timestamp" if published and "T" in published else "unknown",
            category=categorize(title, summary),
            tickers=tickers, rulebased_tickers=tickers,
        ))
    return items


def collect_feed(url: str, source: str, timeout: int = 20) -> list[NewsItem]:
    request = urllib.request.Request(url, headers={"User-Agent": "news-system/0.1"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return parse_rss(response.read(), source)
