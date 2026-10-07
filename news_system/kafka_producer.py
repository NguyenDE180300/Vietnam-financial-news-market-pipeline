"""Publish new RSS articles to Kafka with delivery-backed local state."""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone

from .article_extractor import extract_article
from .batch_collector import CollectorState, bronze_record, news_event_id
from .rss_collector import DEFAULT_FEEDS, collect_feed


def collect_unseen(state: CollectorState):
    candidates = []
    for source, feed_url in DEFAULT_FEEDS.items():
        try:
            items = collect_feed(feed_url, source)
            candidates.extend(state.unseen(items))
            print(f"{source}: fetched={len(items)}", flush=True)
        except Exception as error:
            print(f"[WARN] RSS {source}: {error}", flush=True)
    return list({news_event_id(item): item for item in candidates}.values())


def publish_once(
    bootstrap_servers: str,
    topic: str,
    state: CollectorState,
    full_text: bool = True,
    article_limit: int = 0,
) -> int:
    try:
        from confluent_kafka import Producer
    except ImportError as error:
        raise RuntimeError("Install news_system/requirements-kafka.txt") from error

    items = collect_unseen(state)
    if not items:
        print("No new articles.", flush=True)
        return 0

    fetched = 0
    if full_text:
        for item in items:
            if article_limit > 0 and fetched >= article_limit:
                break
            if not item.url:
                continue
            try:
                article = extract_article(item.url)
                if article.title:
                    item.title = article.title
                if article.content:
                    item.content = article.content
                item.author = article.author
                item.image_url = article.image_url or item.image_url
                fetched += 1
                time.sleep(0.5)
            except Exception as error:
                print(f"[WARN] article {item.url}: {error}", flush=True)

    producer = Producer({
        "bootstrap.servers": bootstrap_servers,
        "client.id": "vn-news-rss-producer",
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "gzip",
    })
    collected_at = datetime.now(timezone.utc)
    delivered_ids: set[str] = set()
    failed: list[str] = []

    def delivery_callback(error, message) -> None:
        event_id = message.key().decode("utf-8") if message.key() else ""
        if error is not None:
            failed.append(f"{event_id}: {error}")
        else:
            delivered_ids.add(event_id)

    by_id = {news_event_id(item): item for item in items}
    for event_id, item in by_id.items():
        record = bronze_record(item, collected_at)
        producer.produce(
            topic,
            key=event_id.encode("utf-8"),
            value=json.dumps(record, ensure_ascii=False).encode("utf-8"),
            on_delivery=delivery_callback,
        )
        producer.poll(0)
    outstanding = producer.flush(30)
    if outstanding:
        raise RuntimeError(f"Kafka delivery timed out for {outstanding} message(s)")

    delivered_items = [by_id[event_id] for event_id in delivered_ids]
    if delivered_items:
        state.mark_emitted(delivered_items, f"kafka://{topic}", collected_at)
    if failed:
        raise RuntimeError("Kafka delivery failed: " + "; ".join(failed[:5]))
    print(
        f"Kafka publish: topic={topic} records={len(delivered_items)} full_text={fetched}",
        flush=True,
    )
    return len(delivered_items)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="news.raw.v1")
    parser.add_argument("--state-db", default="data/kafka_producer_state.db")
    parser.add_argument("--article-limit", type=int, default=0)
    parser.add_argument("--no-full-text", action="store_true")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval-minutes", type=int, default=15)
    args = parser.parse_args()
    if args.interval_minutes < 1:
        parser.error("--interval-minutes must be at least 1")
    state = CollectorState(args.state_db)
    while True:
        publish_once(
            args.bootstrap_servers, args.topic, state,
            full_text=not args.no_full_text,
            article_limit=max(0, args.article_limit),
        )
        if not args.watch:
            break
        print(f"Next Kafka poll in {args.interval_minutes} minute(s).", flush=True)
        time.sleep(args.interval_minutes * 60)


if __name__ == "__main__":
    main()
