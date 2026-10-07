# EDA Report — News–Market Pipeline

Ngày chạy: 07/10/2026 (Asia/Ho_Chi_Minh).

Notebook nguồn: `NEWS_MARKET_EDA.ipynb`. Báo cáo dựa trên Silver/Gold Parquet
local; dataset và notebook có output không được commit lên GitHub.

## 1. Tổng quan

| Dataset | Records | Coverage |
|---|---:|---|
| Silver News | 260 | 5 nguồn |
| Silver Market | 660 | 30 ticker × 22 phiên |
| Gold Event Window | 513 | 57 cặp news–ticker × 9 offset |

Kết luận readiness: dữ liệu hiện đủ để audit pipeline và EDA mô tả, chưa đủ
để huấn luyện mô hình dự đoán đáng tin cậy.

## 2. News quality

- 260/260 `event_id` duy nhất; không trùng URL hoặc `content_hash`.
- Không có dòng invalid hoặc thiếu title.
- Chỉ 2 bài có full text, image và author (0,8%). Nguyên nhân chính là batch
  ban đầu chạy `article-limit=2`; cần thu lại với giới hạn cao hơn trước NLP.
- 47/260 bài có ít nhất một ticker VN30 (18,1%).
- Coverage ticker cao nhất ở `cafef_finance` (30%), sau đó `cafef_stock` (20%).
- Median publish-to-collect latency là 1.416 phút (~23,6 giờ), P95 là 5.816
  phút (~4 ngày). Batch hiện chứa backlog RSS nên chưa đại diện latency realtime.

## 3. Ticker extraction

| Nhóm | Số bài |
|---|---:|
| Không tìm thấy ticker | 213 |
| Rule và NER đồng ý | 38 |
| Rule-only | 8 |
| Partial disagreement | 1 |
| NER-only | 0 |

CRF hiện không bổ sung ticker mới ngoài rule-based. Có một bài liệt kê nhiều mã
mà NER thiếu `BID` so với rule. Trước khi nâng model cần manual review các bài
không có ticker và tập disagreement, sau đó tính entity/ticker precision,
recall và F1 trên nhãn người thật.

## 4. Market quality

- 660 rows, 30 ticker và 22 phiên từ 07/09/2026 đến 06/10/2026.
- Mỗi ticker có đủ 22 phiên; không trùng khóa, không dòng invalid và không có
  phiên cuối tuần.
- 30 giá trị `daily_return` null là phiên đầu tiên của mỗi ticker, đúng thiết kế
  vì không có `previous_close` trong cửa sổ.
- Không có daily return tuyệt đối vượt 10% trong batch.
- SSB có volatility mẫu cao nhất (4,26%), nhưng 22 phiên là quá ngắn để suy
  rộng về rủi ro dài hạn.

## 5. Timezone và point-in-time audit

EDA phát hiện job Gold trước đây đặt Spark session ở `Asia/Ho_Chi_Minh` rồi gọi
`from_utc_timestamp`, làm một số timestamp bị cộng múi giờ hai lần. Job đã được
sửa để giữ Spark session UTC và chuyển sang giờ Việt Nam đúng một lần.

Sau khi rebuild:

- 57 cặp news–ticker.
- 35 cặp có news đăng từ 15:00 trở đi.
- 0 trường hợp T0 đứng trước event date.
- 0 trường hợp anchor T-1 không đứng trước T0.
- Timestamp Việt Nam và `event_date_vn` đã nhất quán.

## 6. Event-window coverage

| Offset | Available | Tỷ lệ |
|---:|---:|---:|
| T-5 đến T0 | 57/57 | 100% |
| T+1 | 30/57 | 52,6% |
| T+2 | 2/57 | 3,5% |
| T+3 | 0/57 | 0% |
| T+5 | 0/57 | 0% |

Mean abnormal return tại T0 là khoảng +0,059% với khoảng tin cậy xấp xỉ
[-0,177%; +0,294%]. T+1 có mean -0,095%, nhưng chỉ có 30 quan sát và khoảng
tin cậy rộng. Không có bằng chứng thống kê đủ mạnh để kết luận tác động tăng hay
giảm từ batch này.

So sánh theo nguồn hoặc thời điểm đăng hiện có nhóm chỉ 1–8 event, vì vậy các
mean khác nhau không nên diễn giải thành hiệu ứng nguồn báo.

## 7. Storage

| Layer | Parquet files | Dung lượng local |
|---|---:|---:|
| Silver News | 5 | 225,9 KiB |
| Silver Market | 22 | 223,9 KiB |
| Gold | 5 | 66,1 KiB |

## 8. Việc nên làm tiếp

1. Chạy collector realtime liên tục và tăng full-article coverage.
2. Backfill market ít nhất 1–3 năm.
3. Mở rộng lên tối thiểu 3.000–5.000 bài và 500–1.000 event có đủ T+5.
4. Manual review ticker extraction và tạo test set độc lập.
5. Chạy lại EDA với coverage T+5 đầy đủ.
6. Chỉ sau đó mới train baseline TF-IDF/Logistic Regression hoặc model market
   feature; dùng time-based split, không random split.
