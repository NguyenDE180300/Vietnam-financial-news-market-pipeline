# Tổng quan hệ thống Vietnam Financial News–Market Pipeline

## 1. Mục tiêu và phạm vi triển khai

Hệ thống thu thập tin tài chính Việt Nam, nhận diện mã cổ phiếu VN30, lấy giá
theo phiên và tạo dataset đo biến động thị trường quanh thời điểm đăng tin.
Kiến trúc hỗ trợ hai luồng:

- **Historical batch:** tạo dữ liệu quá khứ để EDA, gán nhãn và huấn luyện.
- **Realtime/near-realtime:** đọc RSS định kỳ, truyền event qua Kafka và dùng
  Spark Structured Streaming ghi Bronze.

Tài liệu này mô tả **workflow triển khai hoàn chỉnh dùng để thuyết trình và
vận hành hệ thống**. Trong kiến trúc này, local chịu trách nhiệm thu thập/xử lý,
còn ADLS Gen2 là nơi lưu trữ chính thức sau quality gate.

| Hạng mục | Vai trò trong workflow |
|---|---|
| Historical news, full article, ticker extraction | Đã triển khai, có dữ liệu |
| Bronze/Silver News và Market | Đã triển khai bằng Spark local |
| Gold News–Market point-in-time join | Đã triển khai và có quality gate |
| Kafka producer/broker và Spark Kafka consumer | Streaming ingestion cho tin mới |
| ADLS Gen2 | Data Lake chính thức; nhận dữ liệu tự động sau validation |
| Azure VM | Collector nhẹ; Spark/Kafka ưu tiên chạy trên máy local mạnh |
| EDA | Có source, notebook đã chạy và report |
| Mô hình dự đoán tăng/giảm | Chưa triển khai; Gold là dataset đầu vào |

Các service có thể được bật/tắt độc lập mà không làm mất dữ liệu. Đây là hệ
thống nghiên cứu, không phải công cụ khuyến nghị đầu tư.

## 2. Kiến trúc tổng thể

```text
HISTORICAL
Google News RSS theo ticker/tháng
        │
        ▼
SQLite ─► resolve URL + crawl article ─► Rule + CRF NER ─► Bronze News

REALTIME / FUTURE
5 RSS feeds ─► clean/ticker gate/full article ─► Kafka news.raw.v1
                                                        │
                                             Spark Structured Streaming
                                                        │
                                                        ▼
                                                   Bronze News

Bronze News ─► Spark clean/dedup/validate ─► Silver News ───┐
                                                            │
Yahoo Finance ─► Bronze Market ─► Spark ─► Silver Market ───┤
                                                            ▼
                                    Gold News–Market point-in-time join
                                                            │
                                               Quality gates PASSED
                                                            │
                                           automatic versioned ADLS publish
                                                            │
                                                   EDA / dashboard / ML
```

Historical là luồng đã tạo bộ dữ liệu hiện tại. Kafka dành cho dữ liệu mới sau
khi dịch vụ được bật, không thay thế historical backfill.

## 3. Thu thập và làm giàu News

### Historical backfill

`news_system.historical_backfill` truy vấn Google News RSS theo từng mã VN30 và
từng tháng rồi lưu metadata vào SQLite. Mã dùng trong query **không tự động trở
thành nhãn**; title/summary vẫn phải thật sự match mã hoặc alias. Google News
RSS không phải archive hoàn chỉnh nên không bảo đảm coverage tuyệt đối.

```bash
python -m news_system.historical_backfill \
  --vn30 --start 2020-01-01 --end 2026-01-01 --db news_system.db
```

Lệnh mặc định là dry-run; chỉ thêm `--run` sau khi kiểm tra số query và rate
limit.

### Realtime RSS

`news_system.rss_collector` hiện đọc 5 feed: CafeF Chứng khoán, CafeF Doanh
nghiệp, CafeF Tài chính, VnExpress Kinh doanh và VnEconomy Chứng khoán. RSS
thường chỉ có title, URL, thời gian, summary và đôi khi ảnh; vì vậy RSS dùng để
phát hiện tin, còn nội dung phân tích ưu tiên lấy từ trang bài báo.

### Full article

`news_system.crawl_full_articles` xử lý các bài đã phát hiện ticker:

- Resolve URL Google News bằng `googlenewsdecoder`.
- Tải và phân tích DOM bằng `newspaper4k`.
- Lưu URL thật, content, tác giả/ảnh nếu có, trạng thái, lỗi, số lần thử và độ
  dài vào SQLite.
- Chạy lại Rule-based và NER trên toàn văn.
- Giới hạn tốc độ resolver; HTTP 429 dùng exponential backoff.

```bash
python -m news_system.crawl_full_articles \
  --db news_system.db --limit 100 --delay 3 --max-attempts 5 \
  --ner-model models/ticker_ner_crf.joblib
```

Full article là **best-effort**: DOM thay đổi, paywall, anti-bot hoặc bài bị xóa
có thể làm extraction thất bại. Metadata RSS vẫn được giữ để audit, không được
giả thành nội dung đầy đủ.

## 4. Ticker extraction song song

```text
title + summary + content
        ├── Rule-based alias/regex ───────────┐
        └── CRF NER ─► fuzzy entity linker ──┤
                                              ▼
                                     union + whitelist VN30
                                              ▼
                                      merged_tickers
```

- `ticker_extractor.py`: mã, tên công ty và alias; kết quả dễ giải thích.
- `ner_ticker.py`: CRF (`sklearn-crfsuite`) nhận diện entity rồi liên kết về mã.
- `hybrid_ticker_extractor.py`: union hai nhánh và chỉ giữ snapshot VN30.

SQLite/Bronze giữ riêng `rulebased_tickers`, `ner_tickers` và
`merged_tickers`. Model hiện tại được train từ dataset dự án; validation nội bộ
không thay thế human-gold test set độc lập. Binary
`models/ticker_ner_crf.joblib` giữ local, không commit vào Git. Xem
[HYBRID_TICKER_NER.md](news_system/HYBRID_TICKER_NER.md).

## 5. Medallion pipeline

### Bronze News

Bronze là `JSONL.GZ`. `event_id` là SHA-256 của URL, hoặc fallback từ source +
title. Các nhóm trường chính:

| Nhóm | Trường tiêu biểu |
|---|---|
| Định danh | `event_id`, `schema_version`, `event_type` |
| Nguồn | `source`, `url`, `author`, `image_url` |
| Nội dung | `title`, `summary`, `content`, `content_length` |
| Ticker | `rulebased_tickers`, `ner_tickers`, `merged_tickers`, `strategy` |
| Crawl | trạng thái enrichment/extraction và lỗi |
| Thời gian | raw, UTC, giờ Việt Nam, `collected_at_utc`, `ingest_date` |
| Kafka | topic, partition, offset với event streaming |

Historical exporter chỉ phát hành bài có ít nhất một ticker VN30 hợp nhất.

### Silver News

`spark_jobs/bronze_to_silver.py` dùng schema cố định, làm sạch HTML/Unicode và
timestamp; dedup theo `event_id` và ưu tiên full text dài, mới hơn; fallback về
summary; chạy lại Rule + CRF; sinh `content_hash` và validation flags; ghi
Parquet theo `published_year_month`. Quality gate kiểm tra schema, khóa trùng,
timestamp, ticker và record lỗi trước khi dùng cho Gold.

### Bronze/Silver Market

Collector lấy daily adjusted OHLCV của 30 mã VN30 từ Yahoo Finance
(`yfinance`, suffix `.VN`, `auto_adjust=True`). Spark Silver:

- Dedup `(ticker, session_date, interval, source)`.
- Kiểm tra OHLC, volume và phiên giao dịch.
- Tính `previous_close`, `daily_return`, `log_return`.
- Partition theo `session_year_month`.

Nguồn hiện tại là `yahoo_adjusted`. RSI, MACD, volatility, volume ratios mới là
thiết kế, chưa nằm trong Silver; xem
[YAHOO_FINANCE_FEATURE_ENGINEERING.md](news_system/YAHOO_FINANCE_FEATURE_ENGINEERING.md).

### Gold News–Market Impact

`spark_jobs/news_market_gold.py` explode bài theo `merged_tickers` rồi join với
lịch phiên từng mã:

- Event time đổi từ UTC sang `Asia/Ho_Chi_Minh`.
- Tin trước 15:00 có thể dùng phiên cùng ngày làm T0; từ 15:00 dùng phiên kế.
- Offset `T-5, T-3, T-2, T-1, T0, T+1, T+2, T+3, T+5` theo
  `session_index`, không theo ngày lịch.
- Close T-1 là anchor của cumulative return.
- Benchmark là equal-weight VN30 từ cùng dataset, **không phải VN-Index**.
- `abnormal_return = stock_cumulative_return - benchmark_cumulative_return`.
- Phiên chưa tồn tại có `has_market_data=false`; không forward-fill.

Gold là dataset phân tích/ML; abnormal return là tương quan trong event window,
không chứng minh quan hệ nhân quả.

## 6. Kafka và Spark Streaming

Kafka local dùng `apache/kafka:3.9.1`, Kafka UI dùng
`provectuslabs/kafka-ui:v0.7.2`. Producer `confluent-kafka` dùng key
`event_id`, idempotence, `acks=all` và gzip; chỉ cập nhật state DB sau delivery
confirmation.

```bash
docker compose -f news_system/deploy/kafka/compose.yaml up -d

python -m news_system.kafka_producer \
  --bootstrap-servers localhost:9092 --topic news.raw.v1 \
  --state-db data/kafka_producer_state.db --watch --interval-minutes 15

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 \
  news_system/spark_jobs/kafka_to_bronze.py \
  --bootstrap-servers localhost:9092 --topic news.raw.v1 \
  --output data_lake/bronze/news_stream \
  --checkpoint data_lake/checkpoints/news_kafka_to_bronze
```

Checkpoint giữ offset đã hoàn tất. Khi triển khai liên tục, producer và consumer
được quản lý bởi service/timer để tự khởi động lại sau lỗi.

## 7. Lưu trữ local và Azure

```text
news_system.db                              # operational SQLite
data_lake/
├── bronze/news_stream/ingest_date=.../hour=.../*.jsonl.gz
├── bronze/market/ingest_date=.../*.jsonl.gz
├── silver/news/published_year_month=.../*.parquet
├── silver/market_daily/session_year_month=.../*.parquet
├── gold/news_market_impact/gold_year_month=.../*.parquet
├── checkpoints/
└── quarantine/
```

SQLite nên ở filesystem của máy collector, không đặt trực tiếp trong ADLS.
Azure hiện có:

- ADLS Gen2 account `stcitibikedata`; filesystem `bronze`, `silver`, `gold`,
  `checkpoints`.
- VM `vm-citibike-collector-01`, `Standard_B2ats_v2` (2 vCPU, 1 GiB RAM), hiện
  deallocated.

VM đủ cho collector nhỏ nhưng không phù hợp chạy đồng thời Kafka, Spark và UI.
Vì vậy compute nặng chạy ở local, còn ADLS là nơi lưu lake tập trung. Sau khi
các validator trả về `PASSED`, bước publish tự động thực hiện:

```text
Local Silver/Gold
        │
        ▼
ADLS staging/run_id=<UTC timestamp>
        │
        ├── kiểm tra upload, schema, số file và manifest
        ├── FAILED: giữ bản current cũ, ghi log/quarantine
        └── PASSED: cập nhật manifest/current sang run_id mới
```

Layout logic trên ADLS:

```text
abfss://silver@stcitibikedata.dfs.core.windows.net/
├── news/runs/run_id=.../
├── market_daily/runs/run_id=.../
└── manifests/current.json

abfss://gold@stcitibikedata.dfs.core.windows.net/
├── news_market_impact/runs/run_id=.../
└── manifests/current.json
```

Cơ chế versioned publish tránh việc chép Parquet UUID mới lẫn với file cũ gây
nhân đôi dữ liệu. Consumer chỉ đọc version được trỏ bởi `current.json`; nếu
upload lỗi thì phiên bản đang hoạt động không thay đổi. SQLite vẫn nằm trên máy
collector và không cần Azure SQL/PostgreSQL.

## 8. Vận hành batch và tự động publish ADLS

Đồng bộ SQLite historical → Bronze → Silver News → Gold (không refresh Market):

```bash
python -m news_system.sync_historical_lake \
  --db news_system.db --root data_lake \
  --ner-model models/ticker_ner_crf.joblib
```

Refresh Market rồi rebuild cả hai Silver và Gold:

```bash
python -m news_system.run_daily_pipeline --root data_lake --period 1mo
```

Workflow orchestration hoàn chỉnh:

```text
1. Collect News/Market
2. Write Bronze
3. Build Silver News và Silver Market
4. Validate Silver
5. Build Gold News–Market
6. Validate Gold
7. Tạo run_id và upload Silver/Gold vào ADLS staging
8. Verify manifest, schema, file count/checksum
9. Atomically chuyển current sang run_id mới
10. Ghi audit log và gửi cảnh báo nếu thất bại
```

Timer chạy batch gọi một entrypoint duy nhất; chỉ bước 9 xảy ra khi toàn bộ
quality gate và upload verification thành công. Vì vậy EDA/dashboard luôn đọc
phiên bản lake hoàn chỉnh gần nhất, không đọc batch đang upload dở.

Dùng `--dry-run` để xem chuỗi xử lý. Dừng Kafka:

```bash
docker compose -f news_system/deploy/kafka/compose.yaml down
```

Các unit mẫu nằm trong `news_system/deploy/`; phải sửa user, Conda environment
và project path trước khi enable trên máy khác.

## 9. Snapshot dữ liệu gần nhất

| Dataset | Quy mô |
|---|---:|
| SQLite News | 4,836 bài |
| Bài có ticker | khoảng 3,007 |
| Full article trong SQLite | 2,936 |
| Silver News | 2,879 event |
| Silver News có full text | 2,707 (94.03%) |
| Silver News chỉ metadata/summary | 172 |
| Silver Market | 52,142 dòng, 30 ticker, 1,788 phiên |
| Market range | 2019-12-02 → 2026-10-07 |
| Gold | 44,199 dòng; 4,911 event–ticker pairs |
| Gold có market data | 43,867 dòng |

Đây là snapshot, không phải hằng số; cần chạy lại EDA sau mỗi lần cập nhật lake.

## 10. EDA, validation và test

Notebook `.executed.ipynb` có output để xem trực tiếp trên GitHub; notebook
không hậu tố là source để chạy lại:

- [News EDA](eda/NEWS_EDA.executed.ipynb)
- [Market EDA](eda/MARKET_EDA.executed.ipynb)
- [Impact EDA](eda/IMPACT_EDA.executed.ipynb)
- [Combined EDA](eda/COMBINED_EDA.executed.ipynb)

Report diễn giải nằm cạnh notebook dưới tên `*_REPORT.md`.

```bash
python -m unittest discover -s news_system/tests -p 'test_*.py' -v

spark-submit news_system/spark_jobs/validate_silver.py \
  --input data_lake/silver/news
spark-submit news_system/spark_jobs/validate_market_silver.py \
  --input data_lake/silver/market_daily --expected-min-tickers 30
spark-submit news_system/spark_jobs/validate_news_market_gold.py \
  --input data_lake/gold/news_market_impact
```

## 11. Hướng phát triển tiếp theo

1. Hoàn thiện monitoring tập trung cho Kafka/Spark và cảnh báo freshness.
2. Đánh giá NER trên human-gold test set độc lập, theo từng nguồn.
3. Parser riêng cho nguồn extraction kém và theo dõi DOM drift.
4. Sentiment/event classification và mô hình dự đoán impact.
5. Feature kỹ thuật mở rộng trong Market Silver.
6. Data catalog, lineage và retention policy cho các version cũ trên ADLS.
7. Ảnh/video và mạng xã hội trong giai đoạn nghiên cứu sau.

## 12. Tài liệu chi tiết

- [Hybrid local + Azure](news_system/HYBRID_AZURE_SETUP.md)
- [Kafka local](news_system/KAFKA_LOCAL_SETUP.md)
- [Ticker-gated ingestion](news_system/TICKER_GATED_INGESTION.md)
- [Hybrid Rule-based + CRF NER](news_system/HYBRID_TICKER_NER.md)
- [News–Market Impact](news_system/NEWS_MARKET_IMPACT.md)
- [Market feature engineering](news_system/YAHOO_FINANCE_FEATURE_ENGINEERING.md)
- [Media analysis roadmap](news_system/MEDIA_ANALYSIS_ROADMAP.md)
