# Hybrid VN30 Ticker Extraction

## Mục tiêu

Hệ thống chạy hai nhánh độc lập trên cùng một nội dung bài báo:

```text
title + full article (fallback: RSS summary)
              |
       +------+------+
       |             |
  Rule-based       CRF NER
  alias/regex     nhận diện span ORG
       |             |
       |       entity linker -> VN30
       +------+------+
              |
        union + deduplicate
              |
     giữ bài nếu có >= 1 ticker
```

Kết quả được tách thành:

- `rulebased_tickers`: mã tìm thấy bởi regex/alias.
- `ner_tickers`: mã được NER phát hiện và entity linker ánh xạ.
- `merged_tickers`: hợp có thứ tự của hai danh sách trên.
- `ner_entities`: span, offset, ticker được link và độ tin cậy của linker.

Toàn bộ ticker đầu ra bị giới hạn bởi snapshot `VN30_TICKERS` trong
`ticker_extractor.py`.

## Baseline hiện tại

Model tạm là Linear-chain CRF, không phải LLM. CRF học BIO sequence labels
(`B-ORG`, `I-ORG`, `O`) từ `entity_spans_json`. Sau đó entity linker ánh xạ
chuỗi như `Ngân hàng TMCP Á Châu` sang `ACB`. Đây là hai bài toán khác nhau:

1. NER tìm đoạn văn bản là tổ chức/doanh nghiệp liên quan.
2. Entity linking biến đoạn văn bản đó thành mã chứng khoán chuẩn.

Model production được train trên toàn bộ dữ liệu. Trước đó script giữ lại 20%
theo article để báo validation metrics. Metrics này chỉ là baseline vì phần lớn
nhãn hiện tại là weak/assistant labels, chưa phải bộ gold do con người kiểm tra
độc lập.

## Train và test nhanh

```bash
python -m news_system.ner_ticker \
  --data data/labeled_data/ticker_dataset_labeled_rulebase_plus_assistant.csv \
  --output models/ticker_ner_crf.joblib

python -m news_system.hybrid_ticker_extractor \
  --model models/ticker_ner_crf.joblib \
  --text "Ngân hàng TMCP Á Châu tăng vốn, cổ phiếu HPG điều chỉnh"
```

## Lọc database hiện tại

Lệnh sau không xóa SQLite gốc. Nó tạo dataset mới và chỉ ghi những bài có ít
nhất một mã sau khi merge:

```bash
python -m news_system.hybrid_ticker_extractor \
  --db news_system.db \
  --model models/ticker_ner_crf.joblib \
  --output data/news_with_vn30_tickers.jsonl
```

## Collector chạy liên tục

Nên bật `--full-text`; nếu crawl toàn văn thất bại hệ thống mới fallback sang
RSS summary. `--only-with-ticker` khiến bài không có kết quả sau merge không
được ghi vào SQLite.

```bash
python -m news_system.pipeline \
  --watch --interval-minutes 15 \
  --full-text --article-limit 0 \
  --hybrid-model models/ticker_ner_crf.joblib \
  --only-with-ticker \
  --db news_system.db
```

Các cột audit trong SQLite:

- `rulebased_tickers_json`
- `ner_tickers_json`
- `tickers_json` (merged)
- `ticker_extraction_strategy`

## Khi nâng lên PhoBERT

Giữ nguyên `HybridTickerExtractor` và entity linker. Chỉ thay implementation
`CRFTickerNER` bằng một token-classification adapter có cùng hai method
`extract()` và `extract_with_entities()`. Trước khi train model lớn cần ưu tiên
human-review các trường hợp `NER - rule`, trường hợp hai nhánh bất đồng và các
mã VN30 có rất ít span để giảm lệch lớp.
