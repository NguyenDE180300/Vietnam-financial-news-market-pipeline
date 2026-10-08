"""Shared ticker-gated article enrichment for batch and Kafka ingestion."""
from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Callable, Protocol

from .models import NewsItem
from .ticker_extractor import TickerExtractor, VN30_TICKERS


class ArticleResult(Protocol):
    title: str
    content: str
    author: str | None
    image_url: str | None


@dataclass
class PreparationResult:
    ready: list[NewsItem]
    rejected: list[NewsItem]
    deferred: list[NewsItem]
    failed: list[NewsItem]
    attempted: int = 0


def extract_item_tickers(
    item: NewsItem, hybrid_extractor=None, *, include_content: bool = True,
) -> list[str]:
    """Extract VN30 tickers and preserve rule/NER provenance on the item."""
    parts = (item.title, item.summary, item.content) if include_content else (item.title, item.summary)
    text = "\n\n".join(part.strip() for part in parts if part and part.strip())
    if hybrid_extractor is not None:
        result = hybrid_extractor.extract_with_details(text)
        item.rulebased_tickers = result.rulebased_tickers
        item.ner_tickers = result.ner_tickers
        item.tickers = result.merged_tickers
        item.ticker_extraction_strategy = "rulebased+crf_ner"
    else:
        allowed = set(VN30_TICKERS)
        item.rulebased_tickers = [
            ticker for ticker in TickerExtractor().extract(text) if ticker in allowed
        ]
        item.ner_tickers = []
        item.tickers = item.rulebased_tickers
        item.ticker_extraction_strategy = "rulebased"
    return item.tickers


def prepare_ticker_articles(
    items: list[NewsItem],
    *,
    full_text: bool,
    article_limit: int,
    fetch_article: Callable[[str], ArticleResult],
    hybrid_extractor=None,
    can_attempt: Callable[[NewsItem], bool] | None = None,
    record_attempt: Callable[[NewsItem, bool, str | None], None] | None = None,
    minimum_content_length: int = 200,
    request_delay: float = 0.0,
) -> PreparationResult:
    """Gate RSS metadata, enrich eligible news, then confirm its tickers.

    Items beyond ``article_limit`` are deferred instead of being emitted without
    content. Failed items remain un-emitted so a later run can retry them.
    """
    candidates: list[NewsItem] = []
    rejected: list[NewsItem] = []
    for item in items:
        if extract_item_tickers(item, hybrid_extractor, include_content=False):
            candidates.append(item)
        else:
            rejected.append(item)

    if not full_text:
        return PreparationResult(candidates, rejected, [], [], 0)

    ready: list[NewsItem] = []
    deferred: list[NewsItem] = []
    failed: list[NewsItem] = []
    attempted = 0
    for item in candidates:
        if can_attempt is not None and not can_attempt(item):
            deferred.append(item)
            continue
        if article_limit > 0 and attempted >= article_limit:
            deferred.append(item)
            continue
        attempted += 1
        try:
            if not item.url:
                raise RuntimeError("missing article URL")
            article = fetch_article(item.url)
            content = (article.content or "").strip()
            if len(content) < minimum_content_length:
                raise RuntimeError(f"content too short ({len(content)} chars)")
            if article.title:
                item.title = article.title
            item.content = content
            item.author = article.author
            item.image_url = article.image_url or item.image_url
            if extract_item_tickers(item, hybrid_extractor):
                ready.append(item)
            else:
                rejected.append(item)
            if record_attempt is not None:
                record_attempt(item, True, None)
        except Exception as error:
            failed.append(item)
            if record_attempt is not None:
                record_attempt(item, False, str(error))
        if request_delay > 0:
            time.sleep(request_delay)
    return PreparationResult(ready, rejected, deferred, failed, attempted)
