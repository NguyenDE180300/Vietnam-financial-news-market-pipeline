# Workflow hệ thống News–Market

## 1. Kiến trúc

```text
Vietnamese RSS sources
        │
        ▼
RSS + article collector ──► Kafka news.raw.v1
                                   │
                         Spark Structured Streaming
                                   │
                                   ▼
                      Bronze News (JSONL.GZ)
                                   │
                  clean + dedup + schema validation
                  rule-based ticker + CRF NER
                                   │
                                   ▼
                         Silver News (Parquet)
                                   │
                                   │ ticker + event time
                                   ▼
Yahoo daily OHLCV ─► Bronze Market ─► Silver Market
                                   │
                           point-in-time join
                                   │
                                   ▼
                    Gold News–Market Impact
                                   │
                                   ▼
                         Streamlit dashboard
```

Kafka và Spark chạy local để tận dụng tài nguyên máy cá nhân. Azure Data Lake
Gen2 lưu các zone `bronze`, `silver`, `gold` và `checkpoints`.

## 2. News ingestion

`news_system.kafka_producer` đọc năm RSS feed được cấu hình trong
`rss_collector.py`. Mỗi URL tạo một SHA-256 `event_id`; SQLite state chỉ đánh
dấu bài đã emit sau khi Kafka xác nhận delivery.

```bash
python -m news_system.kafka_producer \
  --bootstrap-servers localhost:9092 \
  --topic news.raw.v1 \
  --state-db data/kafka_producer_state.db \
  --watch --interval-minutes 15
```

Full article là best-effort và phụ thuộc từng website. Có thể giới hạn bằng
`--article-limit`; RSS title/summary vẫn được giữ nếu extraction thất bại.

## 3. Kafka → Bronze News

Spark Structured Streaming đọc `news.raw.v1`, giữ metadata Kafka
`topic/partition/offset` và ghi micro-batch JSONL.GZ. Checkpoint ngăn đọc lại
offset đã hoàn tất.

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 \
  news_system/spark_jobs/kafka_to_bronze.py \
  --bootstrap-servers localhost:9092 \
  --topic news.raw.v1 \
  --output data_lake/bronze/news_stream \
  --checkpoint data_lake/checkpoints/news_kafka_to_bronze
```

## 4. Bronze → Silver News

Job `bronze_to_silver.py` thực hiện:

1. Parse schema cố định và timestamp UTC.
2. Làm sạch HTML/khoảng trắng.
3. Dedup theo `event_id`, ưu tiên bản có full text dài hơn.
4. Chạy rule-based VN30 và CRF NER song song.
5. Hợp nhất thành `merged_tickers` và chặn ticker ngoài VN30.
6. Ghi Parquet theo tháng `published_year_month` để tránh tạo quá nhiều file nhỏ.

```bash
spark-submit news_system/spark_jobs/bronze_to_silver.py \
  --input data_lake/bronze/news_stream \
  --output data_lake/silver/news \
  --mode overwrite \
  --ner-model models/ticker_ner_crf.joblib

spark-submit news_system/spark_jobs/validate_silver.py \
  --input data_lake/silver/news
```

Model binary không nằm trong Git. Train model theo
[news_system/HYBRID_TICKER_NER.md](news_system/HYBRID_TICKER_NER.md).

## 5. Market Bronze → Silver

Market collector lấy adjusted daily OHLCV của snapshot VN30. `session_date` là
khóa phiên; `collected_at_utc` là thời điểm ingestion.

```bash
python -m news_system.market_batch_collector \
  --backend local --root data_lake --provider yahoo --vn30 --period 1mo

spark-submit news_system/spark_jobs/market_bronze_to_silver.py \
  --input data_lake/bronze/market \
  --output data_lake/silver/market_daily \
  --mode overwrite

spark-submit news_system/spark_jobs/validate_market_silver.py \
  --input data_lake/silver/market_daily \
  --expected-min-tickers 30
```

Quality gate kiểm tra khóa trùng, OHLC, volume, phiên cuối tuần và return cực
đoan. Yahoo chạy với `auto_adjust=True` để giảm sai lệch do corporate actions.

## 6. Silver → Gold point-in-time join

Event time được đổi từ UTC sang `Asia/Ho_Chi_Minh`:

- Tin trước 15:00 có thể nhận T0 là phiên cùng ngày.
- Tin từ 15:00 trở đi nhận T0 là phiên kế tiếp.
- T+n theo thứ tự phiên thực tế, không cộng ngày lịch.
- Phiên tương lai chưa có để `has_market_data=false`; không forward-fill.

Các offset mặc định: `T-5, T-3, T-2, T-1, T0, T+1, T+2, T+3, T+5`.
Benchmark là equal-weight VN30 từ cùng nguồn market.

```bash
spark-submit news_system/spark_jobs/news_market_gold.py \
  --news-input data_lake/silver/news \
  --market-input data_lake/silver/market_daily \
  --output data_lake/gold/news_market_impact \
  --mode overwrite

spark-submit news_system/spark_jobs/validate_news_market_gold.py \
  --input data_lake/gold/news_market_impact
```

## 7. Daily operation

Sau khi News Bronze đã được đồng bộ về local, một lệnh chạy Market, hai Silver
jobs, Gold và toàn bộ quality gates:

```bash
python -m news_system.run_daily_pipeline
```

Xem trước lệnh bằng `--dry-run`. Timer mẫu nằm trong `news_system/deploy`; cần
đổi đường dẫn/user cho máy triển khai trước khi bật.

## 8. ADLS layout

```text
bronze/news_stream/ingest_date=YYYY-MM-DD/hour=HH/*.jsonl.gz
bronze/market/ingest_date=YYYY-MM-DD/*.jsonl.gz
silver/news/published_year_month=YYYY-MM/*.parquet
silver/market_daily/session_year_month=YYYY-MM/*.parquet
gold/news_market_impact/gold_year_month=YYYY-MM/*.parquet
```

Chỉ upload Silver/Gold sau khi validator tương ứng trả về `PASSED`. Khi rebuild
với `overwrite`, thay thế toàn bộ output prefix tương ứng để file Parquet UUID
cũ không làm dữ liệu bị nhân đôi.

## 9. Testing và dashboard

```bash
python -m unittest discover -s news_system/tests -p 'test_*.py' -v
streamlit run news_system/streamlit_app.py
```

Dashboard đọc SQLite cho news MVP và đọc local Gold Parquet cho biểu đồ event
window. Các chỉ số là công cụ nghiên cứu, không phải khuyến nghị đầu tư.
