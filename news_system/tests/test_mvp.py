import tempfile
import unittest
import gzip
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from news_system.database import NewsDatabase
from news_system.historical_backfill import parse_google_rss
from news_system.models import NewsItem
from news_system.rss_collector import parse_rss
from news_system.ticker_extractor import TickerExtractor
from news_system.ner_ticker import spans_to_bio, tokenize_with_offsets
from news_system.news_market_impact import _known_anchor_index, _t0_index
from news_system.article_extractor import _decode_response_body
from news_system.batch_collector import bronze_record
from news_system.lake_storage import LocalLakeStorage


class NewsMVPTests(unittest.TestCase):
    def test_ticker_alias_and_prefix(self):
        extractor = TickerExtractor()
        self.assertEqual(extractor.extract("HPG/VCB: Hòa Phát và Vietcombank tăng"), ["HPG", "VCB"])
        self.assertEqual(extractor.extract("GDP tăng, USD ổn định, Hòa Phát mở rộng"), ["HPG"])

    def test_parse_and_deduplicate(self):
        xml = '''<rss><channel><item><title>HPG: H&#242;a Ph&#225;t tang</title>
        <description>Hòa Phát công bố cổ tức.</description><link>https://example/a</link>
        <pubDate>Mon, 22 Jan 2024 08:00:00 GMT</pubDate></item></channel></rss>'''.encode()
        item = parse_rss(xml, "test")[0]
        self.assertEqual(item.tickers, ["HPG"])
        self.assertEqual(item.published_at, "2024-01-22T08:00:00+00:00")
        self.assertEqual(item.category, "CỔ TỨC")

        with tempfile.TemporaryDirectory() as directory:
            database = NewsDatabase(Path(directory) / "test.db")
            self.assertIsNotNone(database.add_news(item))
            duplicate = NewsItem(title=item.title, url=item.url, tickers=["FPT"])
            self.assertIsNone(database.add_news(duplicate))
            self.assertEqual(database.stats()["total"], 1)
            self.assertEqual(len(database.search_ticker("FPT")), 1)

    def test_historical_result_does_not_assume_queried_ticker(self):
        xml = '''<rss><channel><item><title>Thị trường thép châu Á biến động</title>
        <description>Nguồn: CafeF</description><link>https://news.google.test/item</link>
        <pubDate>Mon, 22 Jan 2024 08:00:00 GMT</pubDate>
        <source url="https://cafef.vn">CafeF</source></item></channel></rss>'''.encode()
        item = parse_google_rss(xml, "HPG")[0]
        self.assertEqual(item.tickers, [])
        self.assertEqual(item.source, "CafeF")
        self.assertEqual(item.published_precision, "timestamp")

    def test_character_spans_are_aligned_to_bio_tokens(self):
        text = "Ngân hàng Á Châu (ACB) tăng vốn"
        tokens = tokenize_with_offsets(text)
        start = text.index("Ngân hàng Á Châu")
        labels = spans_to_bio(tokens, [{
            "text": "Ngân hàng Á Châu", "ticker": "ACB",
            "start": start, "end": start + len("Ngân hàng Á Châu"),
        }])
        self.assertEqual(labels[:4], ["B-ORG", "I-ORG", "I-ORG", "I-ORG"])

    def test_market_anchor_never_uses_unknown_same_day_close(self):
        bars = [
            (datetime.fromisoformat("2026-09-20").date(), 100.0),
            (datetime.fromisoformat("2026-09-21").date(), 105.0),
        ]
        timezone = ZoneInfo("Asia/Ho_Chi_Minh")
        before_close = datetime(2026, 9, 21, 10, 0, tzinfo=timezone)
        after_close = datetime(2026, 9, 21, 16, 0, tzinfo=timezone)
        self.assertEqual(_known_anchor_index(bars, before_close), 0)
        self.assertEqual(_known_anchor_index(bars, after_close), 1)
        self.assertEqual(_t0_index(bars, before_close), 1)
        self.assertIsNone(_t0_index(bars, after_close))

    def test_gzip_article_response_is_decoded(self):
        html = b"<html><article><p>This is a sufficiently long article paragraph for extraction.</p></article></html>"
        self.assertEqual(_decode_response_body(gzip.compress(html), "gzip"), html)

    def test_bronze_record_and_local_lake_write(self):
        item = NewsItem(
            title="HPG công bố kết quả", source="test", url="https://example.test/a",
            published_at="2026-10-06T09:00:00+07:00",
        )
        collected = datetime.fromisoformat("2026-10-06T02:05:00+00:00")
        record = bronze_record(item, collected)
        self.assertEqual(record["published_at_utc"], "2026-10-06T02:00:00+00:00")
        self.assertEqual(record["published_at_vn"], "2026-10-06T09:00:00+07:00")
        with tempfile.TemporaryDirectory() as directory:
            storage = LocalLakeStorage(directory)
            target = storage.write_bytes(
                "bronze", "news/ingest_date=2026-10-06/batch.jsonl",
                (json.dumps(record) + "\n").encode(),
            )
            self.assertTrue(Path(target).is_file())


if __name__ == "__main__":
    unittest.main()
