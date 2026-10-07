# News System MVP

## Hybrid Azure Student + local Spark

Pipeline batch mới có thể ghi RSS/full article vào Bronze local hoặc ADLS Gen2,
sau đó dùng Spark local tạo Silver Parquet. Hướng dẫn triển khai đầy đủ ở
[HYBRID_AZURE_SETUP.md](HYBRID_AZURE_SETUP.md).

Thử Bronze local:

```bash
python -m news_system.batch_collector --backend local --root data_lake \
  --state-db data_lake/collector_state.db --article-limit 2
```

Azure VM dùng `--backend adls` và xác thực bằng managed identity; không đặt
storage key trong code.

## Kafka + Spark Streaming

Khi môn học yêu cầu streaming, Kafka chạy local và Spark ghi micro-batch vào
ADLS Bronze. Xem [KAFKA_LOCAL_SETUP.md](KAFKA_LOCAL_SETUP.md). Batch collector
vẫn được giữ làm historical backfill/fallback trên Azure VM nhỏ.

Phase 1 + phần adapter Market Data tối thiểu:

- RSS collector không cần `feedparser` (dùng XML parser chuẩn của Python).
- Cleaner HTML/entity.
- Ticker alias + prefix extraction.
- Rule-based category.
- SQLite news và market bars.
- Yahoo Finance adapter tùy chọn.
- Point-in-time event/market join với lợi suất T+1/T+3/T+5 phiên.
- Test ticker và deduplication.

Chạy test:

```bash
python -m unittest discover -s news_system/tests -v
```

Giao diện web thử nghiệm (cài trong environment Conda đang dùng):

```bash
pip install -r news_system/requirements-ui.txt
streamlit run news_system/streamlit_app.py
```

UI cho phép thu RSS/crawl toàn văn có giới hạn, lọc theo nguồn/ticker và xem nội dung, ảnh đã lưu trong SQLite.

Resolve Google News URL và crawl historical full article theo batch:

```bash
python -m news_system.crawl_full_articles \
  --db news_system.db \
  --limit 20 \
  --delay 1
```

Script lưu `resolved_url`, `extraction_status`, `extraction_method`,
`content_length` và `extraction_error`; có thể chạy nhiều batch, chỉ xử lý bài
chưa thành công.

Thu thập news:

```bash
python -m news_system.pipeline --news --db news_system.db
```

Poll RSS liên tục mỗi 15 phút (có thể đổi interval; dừng bằng Ctrl+C):

```bash
python -m news_system.pipeline --watch --interval-minutes 15 --db news_system.db
```

Xem, tìm và export dữ liệu news:

```bash
python -m news_system.pipeline --show --limit 20
python -m news_system.pipeline --ticker HPG --limit 50
python -m news_system.pipeline --stats
python -m news_system.pipeline --export news.csv
```

Thu thập market data (cần `yfinance` trong Conda environment đang dùng):

```bash
python -m news_system.pipeline --market FPT.VN HPG.VN VCB.VN --period 1mo
```

Đồng bộ market data theo ticker đã nhận diện trong news:

```bash
python -m news_system.pipeline --news --full-text --article-limit 5 --db news_system.db
python -m news_system.pipeline --sync-market --period 1mo --db news_system.db
```

`published_at/event_time` giữ offset của nguồn (thường `+07:00` cho Việt Nam), còn
`market_bars.bar_time` được chuẩn hóa về UTC. Khi join, cả hai được parse thành
`datetime` có timezone; horizon `1/3/5 session` là phiên giao dịch, không phải ngày lịch.

Historical market backfill theo khoảng ngày:

```bash
python -m news_system.pipeline --market HPG.VN \
  --start 2024-01-01 --end 2024-02-05
```

Tạo dataset để tự label ticker/NER:

```bash
python -m news_system.build_ticker_dataset \
  --db news_system.db \
  --limit 5000 \
  --output data/ticker_dataset.csv \
  --jsonl data/ticker_dataset.jsonl
```

Mỗi dòng có `text_for_labeling`, `rulebased_tickers`, `manual_tickers`,
`merged_tickers`, `entity_spans_json` và `label_status`. Giữ nguyên kết quả
`rulebased_tickers`; sau khi label, gộp mã vào `merged_tickers` để tạo nhãn
cuối cùng. `predicted_tickers` vẫn được xuất như alias tương thích.

Historical news backfill dùng Google News RSS theo ticker/tháng. Mặc định là dry-run để kiểm tra số query:

```bash
python -m news_system.historical_backfill \
  --tickers HPG FPT VCB \
  --start 2020-01-01 \
  --end 2021-01-01
```

Backfill theo snapshot VN30:

```bash
python -m news_system.historical_backfill \
  --vn30 \
  --start 2020-01-01 \
  --end 2026-01-01 \
  --db news_system.db
```

Lệnh trên là dry-run; thêm `--run` sau khi kiểm tra số query và rate limit.
Danh sách VN30 được lưu thành snapshot trong `ticker_extractor.py` vì HOSE có
thể thay đổi rổ định kỳ.

Chỉ khi đã kiểm tra số query mới thêm `--run` để crawl thật. Query được chia theo từng tháng, có delay giữa các request và dedup theo URL trong SQLite. Ticker chỉ được gắn khi title/summary thực sự match mã hoặc alias; ticker đang query không tự động gán. Lưu ý Google News RSS không đảm bảo là archive đầy đủ.

## Ticker extractor song song: Rule-based + NER

Train CRF NER baseline từ file đã label (chạy trong `tf_linux`):

```bash
python -m news_system.ner_ticker \
  --data data/labeled_data/ticker_dataset_labeled_rulebase_plus_assistant.csv \
  --output models/ticker_ner_crf.joblib
```

Hai nhánh chạy độc lập rồi hợp nhất:

```text
article -> rule-based -------------------+
        -> CRF NER -> entity linker -----+-> union VN30 -> keep/drop
```

Xuất lại SQLite hiện có thành JSONL, chỉ giữ bài có ít nhất một ticker sau khi gộp:

```bash
python -m news_system.hybrid_ticker_extractor \
  --db news_system.db \
  --model models/ticker_ner_crf.joblib \
  --output data/news_with_vn30_tickers.jsonl
```

Thu RSS liên tục, ưu tiên toàn văn, chạy hai nhánh và **chỉ lưu bài có ticker**:

```bash
python -m news_system.pipeline \
  --watch --interval-minutes 15 --full-text \
  --hybrid-model models/ticker_ner_crf.joblib \
  --only-with-ticker --db news_system.db
```

SQLite lưu riêng `rulebased_tickers_json`, `ner_tickers_json` và kết quả hợp nhất
trong `tickers_json`. CRF là baseline NER không dùng LLM; khi đủ nhãn thật có thể
thay bằng PhoBERT mà không đổi quy tắc merge/lọc.

## Đo tác động News → Market

Backfill giá cho các mã VN30 xuất hiện trong news và VN-Index, sau đó tính return
T+1/T+3/T+5 phiên:

```bash
python -m news_system.news_market_impact \
  --db news_system.db --sync-market \
  --output data/news_market_impacts.csv
```

Hệ thống dùng giá VCI/vnstock và giá đóng cửa cuối cùng đã biết tại thời điểm đăng tin làm anchor.
Tin trước 15:00 không được phép dùng giá đóng cửa cùng ngày. `abnormal_return`
bằng return cổ phiếu trừ return VN-Index trên cùng cửa sổ; nhãn mặc định là
`positive` nếu >= 2%, `negative` nếu <= -2%, còn lại là `neutral`. Kết quả cũng
được lưu vào bảng SQLite `news_market_impacts`.

## Phạm vi hiện tại

Pipeline Data Engineering đã có Kafka, Spark Structured Streaming, Bronze,
Silver News/Market và Gold point-in-time join. Full article extraction là
best-effort theo từng nguồn; phân tích ảnh/video và sentiment/PhoBERT vẫn là
hướng nghiên cứu tương lai được mô tả trong
[MEDIA_ANALYSIS_ROADMAP.md](MEDIA_ANALYSIS_ROADMAP.md). Facebook chưa tích hợp.
