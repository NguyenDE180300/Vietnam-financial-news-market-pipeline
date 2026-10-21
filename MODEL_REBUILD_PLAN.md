# Kế hoạch làm lại mô hình News → Stock Direction

Ngày cập nhật: 21/10/2026

## 1. Kết luận từ hệ thống hiện tại

Pipeline kỹ thuật đã chạy được từ news, market data, ticker extraction, Bronze/Silver/Gold đến Spark training. Tuy nhiên, kết quả hiện tại chưa đủ để khẳng định mô hình dự báo được hướng giá.

Các phát hiện chính:

- Accuracy T+1 từng đạt 57.84%, nhưng test khi đó chỉ có 204 event–ticker rows trong tám ngày.
- Sau khi mở rộng test lên 549 eligible rows, `text + market` T+1 còn 55.37%, thấp hơn majority baseline 58.65%.
- Không feature set nào thắng ổn định trên validation và test ở cả T+1, T+3 và T+5.
- Thứ hạng model thay đổi mạnh theo market regime.
- Historical-news context đôi lúc cải thiện macro-F1, đặc biệt T+3, nhưng chưa ổn định.
- PhoBERT hiện chỉ encode `title + summary`; ảnh chưa tham gia model.
- Vấn đề chính hiện tại là dữ liệu, target alignment và evaluation protocol, không phải thiếu epoch.

Vì vậy hướng làm lại nên bắt đầu từ data contract và evaluation, sau đó mới tăng độ phức tạp của model.

## 2. Mục tiêu phiên bản mới

Xây dựng một hệ thống có thể trả lời trung thực:

> Tại thời điểm bài báo được công bố, thông tin đang có giúp dự đoán giá cổ phiếu tăng hay giảm trong T+1, T+3 và T+5 tốt hơn baseline đơn giản hay không?

Điều kiện thành công:

- Không dùng dữ liệu xuất hiện sau prediction time.
- Kết quả ổn định trên nhiều temporal fold.
- So sánh với majority, persistence và market-only baseline.
- Báo cáo macro-F1, balanced accuracy, F1 từng lớp và confidence interval.
- Model phải cải thiện trên nhiều fold, không chỉ một test window.

## 3. Làm lại data layer

### 3.1. Tách rõ hai luồng dữ liệu

**Historical backfill**

- Google News RSS để tìm URL lịch sử.
- Resolve URL và crawl publisher page.
- Lưu raw response, `collected_at`, `published_at`, `updated_at` nếu có.
- Không coi Google News là nguồn publication cuối cùng.

**Realtime ingestion**

- Năm RSS: CafeF Stock, CafeF Company, CafeF Finance, VnExpress Business và VnEconomy Stock.
- Kafka nhận event mới.
- Spark Structured Streaming ghi Bronze.
- Crawl full article ngay sau khi nhận URL và lưu snapshot đầu tiên.

Hai luồng phải đi qua cùng một Silver schema để giảm distribution mismatch.

### 3.2. News quality gate

Chỉ đưa bài vào training candidate nếu:

- Có `published_at` hợp lệ và timezone rõ ràng.
- Có ticker VN30 sau bước Rule + NER + entity linking.
- Full content ít nhất 200 ký tự.
- URL/canonical URL hợp lệ.
- Không trùng `content_hash`.
- Không phải navigation page, tag page hoặc nội dung lỗi parser.
- Có dấu vết parser, thời điểm crawl và trạng thái extraction.

Không dùng RSS snippet thay cho full article khi train model chính. Có thể giữ snippet trong Bronze để audit.

### 3.3. Snapshot và nguy cơ bài bị sửa

Đây là phần cần bổ sung để tránh leakage khó nhìn thấy:

- Lưu `first_collected_at` và raw HTML lần đầu.
- Tính hash cho từng content snapshot.
- Nếu bài được cập nhật, lưu version mới thay vì ghi đè.
- Model chỉ được dùng version có `collected_at <= prediction_time + allowed_delay`.
- Historical article không có snapshot đúng thời điểm phải được đánh dấu `historical_snapshot_unverified`.

### 3.4. Ticker quality

Một bài có thể nhắc nhiều công ty nhưng không phải ticker nào cũng là đối tượng chịu tác động chính. Cần ba mức:

- `primary_ticker`: đối tượng chính của bài;
- `mentioned_tickers`: chỉ được nhắc đến;
- `sector_or_market_event`: tin vĩ mô/ngành ảnh hưởng nhiều mã.

Ưu tiên train trên `primary_ticker`. Multi-ticker rows chỉ được tạo khi có evidence span và relation hợp lý.

### 3.5. Market data contract

Tất cả feature phải có `available_at`:

- Tin trước close: anchor là close phiên trước.
- Tin sau close: anchor có thể là close cùng ngày.
- T+1/T+3/T+5 là phiên thứ 1/3/5 sau `event_date` theo trading calendar.
- Không dùng target close, future volume hoặc rolling window chứa phiên tương lai.
- Corporate action hoặc price anomaly phải mask target.

Nên thêm benchmark return và sector return, nhưng chỉ từ dữ liệu đã biết tại prediction time.

## 4. Định nghĩa lại dataset và split

### 4.1. Đơn vị quan sát

Đơn vị chính là `event_id × primary_ticker`, nhưng metric phải báo cáo ở ba mức:

1. row-level;
2. unique event-level;
3. ticker-date cluster-level.

Điều này tránh việc một bài gắn nhiều ticker hoặc nhiều bài cùng ngày làm tăng giả số lượng quan sát độc lập.

### 4.2. Walk-forward evaluation

Không dùng duy nhất một train/validation/test split. Đề xuất:

| Fold | Train | Validation | Test |
|---|---|---|---|
| 1 | 2020–2022 | H1/2023 | H2/2023 |
| 2 | 2020–2023 | H1/2024 | H2/2024 |
| 3 | 2020–2024 | H1/2025 | H2/2025 |
| 4 | 2020–2025 | H1/2026 | H2/2026 |

Ngày cụ thể phải được điều chỉnh để mỗi fold có đủ số event và đủ hai lớp. Không tune threshold trên test fold.

### 4.3. Group-aware split

Trong mỗi mốc thời gian:

- Không để cùng `event_id` xuất hiện ở hai split.
- Không để exact/near duplicate content qua split.
- Có embargo giữa train và validation/test tối thiểu bằng horizon lớn nhất.
- History context của một row chỉ được xây từ bài trước prediction time.

## 5. Target mới

### 5.1. Target chính

Giữ bài toán nhị phân để có baseline rõ ràng:

- `decrease`: raw return `< 0`;
- `increase`: raw return `> 0`;
- exact zero hoặc missing: mask.

Train ba head T+1, T+3 và T+5. Không đưa T0 vào phiên bản đầu.

### 5.2. Target phụ cần thử

Raw sign dễ nhiễu vì biến động rất nhỏ cũng thành tăng/giảm. Sau baseline, thử:

- abnormal return so với VN-Index;
- return trừ sector return;
- volatility-adjusted return;
- binary target chỉ giữ mẫu `abs(return) >= threshold`, còn vùng nhỏ bị mask.

Threshold chỉ được chọn bằng train/validation, không nhìn test.

## 6. Feature ablation bắt buộc

Mọi cấu hình dùng cùng split, target, seed và classifier:

| ID | Feature set | Mục đích |
|---|---|---|
| A0 | Majority/persistence | Baseline tối thiểu |
| A1 | Market-only | Đo tín hiệu giá quá khứ |
| A2 | Text-only | Đo riêng tín hiệu news |
| A3 | Text + market | Đo đóng góp market cho news |
| A4 | Text + market + history | Đo đóng góp related news |
| A5 | Text + market + event type | Đo loại sự kiện |
| A6 | Text + market + history + image | Multimodal cuối cùng |

Không thêm ticker/source metadata vào một số model mà không báo rõ, vì điều đó làm ablation không còn sạch.

## 7. Text và historical-news representation

### 7.1. Text encoder

Thực hiện theo hai tầng:

1. Frozen PhoBERT embeddings + Logistic Regression/MLP để làm baseline.
2. Chỉ fine-tune end-to-end sau khi baseline và evaluation ổn định.

Input cần thử riêng:

- title;
- title + summary;
- title + cleaned full content;
- hierarchical chunks cho bài dài.

Không mặc định rằng full content luôn tốt hơn; phải ablation.

### 7.2. Related-news retrieval

Similarity chỉ dùng text, không dùng ảnh. Candidate phải cùng ticker và nằm trước event.

Thay vì cố định 0.30–0.85 mãi mãi:

- tạo labeled validation nhỏ gồm related/unrelated/duplicate;
- tune lower threshold để lọc unrelated;
- tune upper threshold để loại syndicated/near duplicate;
- so sánh top-k, recency decay và event-type consistency.

History vector có thể dùng attention hoặc recency/similarity weighted pooling sau khi baseline weighted mean được xác nhận.

## 8. Image branch

Ảnh không tham gia tìm related news. Ảnh chỉ là feature cho prediction:

```text
text embedding ---------+
market features --------+--> fusion --> T+1/T+3/T+5
history text embedding -+
image embedding + mask -+
```

Yêu cầu:

- Pretrained image encoder frozen trước.
- Có `image_present_mask`.
- Ảnh logo/template/stock photo cần được đánh dấu hoặc deduplicate bằng perceptual hash.
- So sánh A4 và A6 để chứng minh ảnh có đóng góp.

Nếu ảnh không cải thiện walk-forward mean macro-F1 thì loại khỏi model chính.

## 9. Model strategy

Thứ tự triển khai:

1. Logistic Regression làm reference baseline.
2. Gradient-boosted tree cho market/tabular branch.
3. MLP fusion cho frozen embeddings.
4. Multi-task neural model dùng shared encoder và ba horizon heads.
5. End-to-end PhoBERT fine-tuning chỉ sau khi có đủ dữ liệu và baseline đáng tin.

Spark được dùng cho ETL, feature generation, joins và baseline training. Neural model nên train bằng PyTorch trên GPU; không cần ép PhoBERT training vào Spark.

## 10. Evaluation protocol

Metric bắt buộc cho từng horizon và fold:

- accuracy;
- balanced accuracy;
- macro-F1;
- F1/precision/recall từng lớp;
- confusion matrix;
- majority accuracy;
- bootstrap confidence interval theo event và ticker-date;
- calibration/Brier score nếu xuất probability.

Báo cáo mean ± standard deviation qua các fold. Một model chỉ được xem là tốt hơn baseline nếu:

- cải thiện macro-F1 trên phần lớn fold;
- confidence interval không cho thấy cải thiện chỉ đến từ một vài ngày;
- không đánh đổi hoàn toàn một lớp để tăng accuracy;
- giữ kết quả trên untouched final holdout.

## 11. Negative controls và leakage tests

Trước mỗi vòng train chính:

1. Shuffle label trong train: kết quả phải trở về chance.
2. Future-feature scan: mọi feature phải có `available_at <= prediction_time`.
3. Duplicate scan: URL, canonical URL, content hash, normalized title và embedding similarity.
4. Train model chỉ bằng source/date/ticker metadata để phát hiện shortcut.
5. Reverse-time test: nếu kết quả bất thường cao, kiểm tra target join.
6. Kiểm tra article snapshot có nội dung cập nhật sau publication hay không.

## 12. Kế hoạch thực hiện

### Phase A — Data repair

- Crawl tiếp full article còn chờ trong SQLite theo batch có rate limit.
- Lưu snapshot metadata và parser audit fields.
- Làm sạch primary ticker và event type.
- Đồng bộ historical/realtime về cùng schema.

Deliverable: Gold v3 có data-quality report và leakage audit.

### Phase B — Evaluation foundation

- Xây walk-forward folds.
- Thêm embargo và group-aware dedup.
- Viết majority, persistence và market-only baseline.
- Thêm clustered bootstrap.

Deliverable: reproducible evaluation command và baseline report.

### Phase C — Text and history

- So sánh title, summary và full content.
- Tune retrieval threshold trên labeled retrieval validation.
- Chạy A2–A5 qua toàn bộ folds.

Deliverable: ablation table có mean ± standard deviation.

### Phase D — Multimodal

- Deduplicate ảnh.
- Sinh frozen image embeddings.
- Train fusion model với image mask.
- Chạy A6 và so với A4.

Deliverable: multimodal contribution report.

### Phase E — Final model

- Chọn kiến trúc dựa trên validation folds.
- Train lại trên train + validation history.
- Chấm đúng một lần trên untouched holdout.
- Đóng gói inference contract và model card.

## 13. Tiêu chí dừng hoặc tiếp tục

Tiếp tục đầu tư model phức tạp khi:

- dataset có đủ nhiều unique events ở mỗi fold;
- text+market thắng market-only và majority trên phần lớn folds;
- macro-F1 cải thiện có confidence interval hợp lý;
- kết quả không phụ thuộc một source hoặc vài ngày.

Nếu các điều kiện trên chưa đạt, ưu tiên thêm/chỉnh dữ liệu và target thay vì tăng epoch hoặc đổi sang model lớn hơn.

## 14. Trạng thái artifact hiện tại

- Gold mới: `data_lake/gold/training_targets_phobert_simcse_e5_030_085_v2`
- Fixed-split models: `models/ablation_binary_v2_*`
- Rolling models: `models/ablation_binary_rolling_v3_*`
- Leakage audit: `news_system/LEAKAGE_AUDIT_BINARY_ABLATION.md`
- Retrain report: `news_system/BINARY_ABLATION_RETRAIN_REPORT.md`

Model binaries, raw data, SQLite và downloaded images không nên commit trực tiếp lên GitHub. Chỉ commit code, schema, configuration và reports; artifact lớn nên đặt tại ADLS hoặc một model registry.
