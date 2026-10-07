# Mở rộng News System: nội dung bài viết và phân tích ảnh/video

## Mục tiêu

RSS hiện phù hợp để phát hiện bài mới và lấy metadata/summary, nhưng thường không chứa toàn văn, ảnh gốc hoặc video đầy đủ. Hệ thống tương lai cần lấy nội dung trang bài viết và metadata media hợp lệ, sau đó phân tích cùng nhau để tạo dữ liệu nghiên cứu cho project sau.

## Pipeline đề xuất

```text
RSS discovery
    ↓
Lưu metadata + canonical article URL
    ↓
Article fetcher (rate limit, cache, source-specific parser)
    ├── title / body / author / published time
    ├── canonical URL / source / category
    └── media references: image, video, caption, thumbnail
    ↓
Normalize + deduplicate + provenance
    ↓
Text / image / video analysis jobs
    ├── Text: event, ticker, sentiment, numbers, entities
    ├── Image: caption, OCR, chart/table detection
    └── Video: metadata, sampled frames, OCR/caption, transcript nếu có
    ↓
Evidence-linked structured event
    ↓
Market data / event study / research project
```

## Nguyên tắc thu thập nội dung

- RSS là discovery layer; `article_url` là nguồn canonical để lấy chi tiết.
- Dùng parser theo từng publisher thay vì một CSS selector áp dụng cho mọi site.
- Ưu tiên lấy `articleBody`, Open Graph/JSON-LD metadata, ảnh đại diện và URL video được nhúng công khai.
- Tôn trọng điều khoản sử dụng, robots policy, copyright, rate limits và quyền truy cập của publisher.
- Không vượt đăng nhập/paywall, không né CAPTCHA hoặc cơ chế chống bot.
- Không tải video dung lượng lớn mặc định. Lưu URL, metadata và thumbnail; chỉ tải/phân tích khi được phép và có nhu cầu nghiên cứu cụ thể.
- Lưu thời gian fetch, nguồn, URL gốc, hash nội dung và trạng thái parser để audit/replay.
- Lưu raw payload chỉ khi quyền và chính sách lưu trữ cho phép; nếu không thì lưu URL, metadata và nội dung trích xuất tối thiểu.

## Data model mở rộng

### `articles`

- `news_id`
- `canonical_url`
- `body_text`
- `author`
- `published_at`
- `fetched_at`
- `body_hash`
- `extractor_name` / `extractor_version`
- `fetch_status` / `fetch_error`

### `media_assets`

- `media_id`, `news_id`
- `media_type`: `image`, `video`, `audio`, `chart`, `unknown`
- `url`, `thumbnail_url`, `mime_type`
- `caption`, `alt_text`, `duration_seconds`
- `content_hash`, `rights_status`, `fetch_status`

### `media_analysis`

- `media_id`, `model_name`, `model_version`
- `task`: `caption`, `ocr`, `chart_parse`, `frame_summary`, `transcription`
- `result_json`, `confidence`
- `evidence`: frame/time range hoặc vùng ảnh liên quan
- `created_at`

Không ghi kết quả model đè lên raw text/media metadata; giữ output theo model version để có thể đánh giá lại.

## Cách phân tích media

### Ảnh

- Image captioning để mô tả nội dung tổng quát.
- OCR để đọc chữ, số, mã cổ phiếu và nhãn trục.
- Phân loại ảnh thường, infographic, bảng số liệu hoặc biểu đồ.
- Nếu là chart: lưu kết quả trích xuất có cấu trúc kèm đơn vị, trục, thời gian và confidence; không coi OCR đơn thuần là giá dữ liệu chuẩn.

### Video

- Trước hết lấy title, description, duration, thumbnail và transcript/caption sẵn có nếu truy cập hợp lệ.
- Nếu cần phân tích hình ảnh, lấy frame thưa theo khoảng thời gian hoặc tại scene change; tránh gửi toàn bộ video vào model.
- Gắn timestamp cho từng frame/transcript segment để mọi kết luận có thể truy ngược evidence.
- Không suy ra diễn biến giá/chỉ dẫn đầu tư chỉ từ hình ảnh; đối chiếu nội dung text và market data riêng.

## Kết quả model nên có dạng

```json
{
  "event_type": "earnings_announcement",
  "tickers": ["HPG"],
  "claims": [
    {
      "claim": "Doanh nghiệp công bố lợi nhuận quý tăng",
      "evidence_type": "article_text",
      "evidence_ref": "paragraph:4",
      "confidence": 0.91
    },
    {
      "claim": "Ảnh đính kèm là biểu đồ doanh thu theo quý",
      "evidence_type": "image_ocr_caption",
      "evidence_ref": "media:img-1, region:(x1,y1,x2,y2)",
      "confidence": 0.84
    }
  ],
  "model_version": "..."
}
```

Model tạo **nhãn/sự kiện và giả thuyết nghiên cứu có evidence**, không đưa ra kết luận nhân quả rằng bài đăng/tin tức làm giá thay đổi. Phần đó cần event study, benchmark và kiểm soát yếu tố khác.

## Triển khai theo giai đoạn

1. **News MVP hiện tại:** RSS metadata/summary, cleaner, ticker rules, category, SQLite, dedup.
2. **Article enrichment:** fetch toàn văn HTML có chọn lọc, parser theo nguồn, lưu canonical URL và provenance.
3. **Media inventory:** lấy tham chiếu ảnh/video/thumbnail/caption, chưa chạy model.
4. **Multimodal prototype:** OCR + image captioning trước; video chỉ lấy transcript/frame mẫu.
5. **Research dataset:** event labels/evidence join với market bars; đánh giá extraction và độ nhạy kết quả.

Chỉ mở rộng sang Kafka/Spark, object storage hoặc Azure media workloads khi volume và yêu cầu lưu trữ đã rõ. Không tải hoặc lưu hàng loạt media trước khi xác nhận quyền, chi phí và mục tiêu nghiên cứu.

## Trạng thái

Đây là roadmap, **chưa được triển khai**. MVP hiện tại chỉ lấy RSS và chưa crawl full article, chưa lưu media assets, chưa gọi image/video models.
