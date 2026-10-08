# Ticker-gated full-article ingestion

## Mục tiêu

Chỉ phát hành tin liên quan VN30 vào Bronze/Kafka. Khi bật full-text, một bài
chỉ được phát hành sau khi crawler lấy được ít nhất 200 ký tự nội dung.

```text
RSS title + summary
        |
        v
Rule-based + CRF NER gate ---- không có VN30 ---> filtered_news
        |
        v
Article queue (giới hạn mỗi lượt)
        | crawl lỗi                         | vượt article-limit
        v                                   v
retry, tối đa 5 lần                     để batch sau xử lý
        |
        v
title + summary + full content
        |
        v
Rule-based + CRF NER lần 2
        |
        v
Bronze JSONL.GZ hoặc Kafka news.raw.v1
        |
        v
Spark Silver (mặc định chỉ giữ bài có VN30)
```

## Trạng thái local

SQLite state DB của collector có ba bảng:

- `emitted_news`: đã ghi Bronze hoặc Kafka xác nhận delivery.
- `filtered_news`: không có ticker VN30 sau bước extraction.
- `article_enrichment_attempts`: số lần crawl, trạng thái và lỗi gần nhất.

Bài vượt `article-limit` hoặc crawl lỗi không được đánh dấu emitted. Nó sẽ được
thử trong lượt sau. Sau 5 lần lỗi, bài được giữ trong state để audit nhưng không
tiếp tục tạo request tự động.

## Batch local hoặc ADLS

```bash
python -m news_system.batch_collector \
  --backend local \
  --root data_lake \
  --state-db data/collector_state.db \
  --article-limit 20 \
  --ner-model models/ticker_ner_crf.joblib
```

Đổi `--backend local` thành `--backend adls --account-name <storage-account>`
để ghi thẳng lên ADLS. Không dùng `--no-full-text` nếu Bronze yêu cầu toàn văn.

## Kafka realtime

```bash
python -m news_system.kafka_producer \
  --bootstrap-servers localhost:9092 \
  --topic news.raw.v1 \
  --state-db data/kafka_producer_state.db \
  --article-limit 20 \
  --ner-model models/ticker_ner_crf.joblib \
  --watch --interval-minutes 15
```

Kafka chỉ nhận bài đã qua ticker gate và full-article gate. State chỉ chuyển
sang emitted sau khi broker xác nhận delivery.

## Enrich dữ liệu lịch sử trong SQLite

Lệnh dưới chỉ chọn các dòng có `tickers_json != '[]'`, lưu số lần thử và chạy
lại Rule + NER sau khi tải nội dung:

```bash
python -m news_system.crawl_full_articles \
  --db news_system.db \
  --limit 20 \
  --delay 1.5 \
  --max-attempts 5 \
  --ner-model models/ticker_ner_crf.joblib \
  --watch --interval-minutes 15
```

Nên chạy batch nhỏ, kiểm tra điều khoản của từng nguồn và không giảm delay để
tránh gây tải cho website. Sau khi crawl đủ, export lại Bronze:

```bash
python -m news_system.export_sqlite_to_bronze \
  --db news_system.db \
  --backend local \
  --root data_lake \
  --batch-size 1000
```

Exporter mặc định chỉ đưa bài có ticker vào Bronze lịch sử.

## Rebuild Silver

```bash
spark-submit news_system/spark_jobs/bronze_to_silver.py \
  --input data_lake/bronze/news_stream \
  --output data_lake/silver/news \
  --mode overwrite \
  --ner-model models/ticker_ner_crf.joblib
```

Silver mặc định loại bài không có ticker VN30. Chỉ dùng
`--include-without-ticker` khi cần tạo tập negative riêng cho nghiên cứu NER.

Đồng bộ trọn gói SQLite đã enrich sang Bronze, Silver và Gold local:

```bash
python -m news_system.sync_historical_lake \
  --db news_system.db --root data_lake \
  --ner-model models/ticker_ner_crf.joblib
```

## Chỉ số cần theo dõi

- Số RSS candidates và tỷ lệ qua ticker gate.
- Số bài `ready`, `deferred`, `failed`, `rejected` mỗi lượt.
- Full-text success rate theo nguồn.
- Số bài đạt `content_length >= 200`.
- Rule/NER disagreement trước và sau full article.
- Số bài đạt giới hạn 5 lần thử để sửa parser theo nguồn.
