# Vietnamese Financial News–Market Pipeline

Data Engineering MVP thu thập tin tài chính Việt Nam, nhận diện ticker VN30,
đồng bộ dữ liệu giá và xây dựng event-window dataset để nghiên cứu phản ứng thị
trường.

Pipeline sử dụng Kafka, Apache Spark và kiến trúc Bronze/Silver/Gold trên Azure
Data Lake Gen2. Kafka và Spark có thể chạy trên máy local; Azure Student chỉ cần
làm lớp lưu trữ.

## Thành phần chính

- RSS collector và full-article extraction có giới hạn.
- Kafka producer idempotent và Spark Structured Streaming.
- News Silver: clean, dedup, data quality, rule-based + CRF NER ticker.
- Market Silver: VN30 daily OHLCV, adjusted return và quality gate.
- Gold: point-in-time join theo múi giờ Việt Nam và phiên giao dịch.
- Streamlit dashboard cho news và event-window impact.

Đọc [WORKFLOW.md](WORKFLOW.md) để xem kiến trúc, schema, thứ tự chạy và quy tắc
không look-ahead. Hướng dẫn Azure chi tiết nằm tại
[news_system/HYBRID_AZURE_SETUP.md](news_system/HYBRID_AZURE_SETUP.md).

## Cài đặt nhanh

Yêu cầu Python 3.10+, Java 17, Docker và Spark 3.5.7.

```bash
python -m pip install -r requirements.txt
python -m unittest discover -s news_system/tests -p 'test_*.py' -v
```

Khởi động Kafka:

```bash
docker compose -f news_system/deploy/kafka/compose.yaml up -d
```

Chạy producer news liên tục:

```bash
python -m news_system.kafka_producer \
  --bootstrap-servers localhost:9092 \
  --topic news.raw.v1 \
  --state-db data/kafka_producer_state.db \
  --watch --interval-minutes 15
```

Chạy daily pipeline sau khi đã có News Bronze local:

```bash
python -m news_system.run_daily_pipeline
```

Mở dashboard:

```bash
streamlit run news_system/streamlit_app.py
```

## Dữ liệu và bảo mật

Repository không chứa article dataset, SQLite state, Data Lake output,
checkpoint, Azure credential hay trained-model binary. Các artefact này được
tạo local hoặc lưu trên ADLS. Không commit storage key, SAS token hay API key.

## Giới hạn nghiên cứu

Abnormal return thể hiện tương quan trong event window, không tự chứng minh bài
báo gây ra biến động giá. RSS không phải kho lưu trữ lịch sử đầy đủ; kết quả NER
cần được đánh giá lại trên tập nhãn do con người kiểm chứng.
