# EDA Report — News–Market Pipeline

Ngày chạy: 07/10/2026 (Asia/Ho_Chi_Minh).

Notebook nguồn: `NEWS_MARKET_EDA.ipynb`. Báo cáo dựa trên Silver/Gold Parquet
local; dataset và notebook có output không được commit lên GitHub.

## 1. Tổng quan

| Dataset | Records | Coverage |
|---|---:|---|
| Silver News | 3.003 | 176 nhãn nguồn, 01/01/2020–06/10/2026 |
| Silver Market | 52.142 | 30 ticker, 1.788 ngày giao dịch, 02/12/2019–07/10/2026 |
| Gold Event Window | 29.997 | 3.333 cặp news–ticker × 9 offset |

So với batch EDA ban đầu, News ticker-qualified tăng từ 47 lên 3.003, Market từ
660 lên 52.142 và Gold từ 513 lên 29.997 dòng. Dữ liệu hiện đủ để audit pipeline,
EDA mô tả và xây baseline nghiên cứu; chưa đủ sạch để xem kết quả là bằng chứng
nhân quả hoặc đưa mô hình vào production.

## 2. News quality

- 3.003/3.003 `event_id` duy nhất; không trùng URL, có 1 cặp trùng
  `content_hash` cần review.
- Không có dòng invalid hoặc thiếu title.
- 16 bài có full text, image và author (0,53%). Phần lịch sử chủ yếu là
  metadata/snippet, vì vậy chưa nên train NLP trên nội dung đầy đủ.
- 3.003/3.003 bài có ít nhất một ticker VN30 (100%); Silver mặc định đã loại
  bài không qua ticker gate.
- Có 176 nhãn nguồn do dữ liệu lịch sử mang tên publisher không chuẩn hóa hoàn
  toàn; nên thêm bảng ánh xạ publisher canonical trước phân tích theo nguồn.
- Chỉ 119 dòng có thể tính publish-to-collect latency. Median là 1.532 phút
  (~25,5 giờ), P95 là 5.818 phút (~4 ngày); đây là backlog lịch sử, không đại
  diện latency realtime.

## 3. Ticker extraction

| Nhóm | Số bài |
|---|---:|
| Rule và NER đồng ý | 1.721 |
| Rule-only | 1.228 |
| Partial disagreement | 44 |
| NER-only | 10 |

NER đã bổ sung ticker cho 10 bài mà rule-based không tìm thấy. Tuy nhiên, cần
manual review toàn bộ 54 dòng NER-only/disagreement và lấy mẫu rule-only trước
khi dùng làm nhãn huấn luyện. Chỉ có nhãn người thật mới cho phép báo cáo
precision, recall và F1 đáng tin cậy.

## 4. Market quality

- 52.142 dòng hợp lệ, 30 ticker và 1.788 ngày giao dịch từ 02/12/2019 đến
  07/10/2026.
- Không trùng khóa, không dòng invalid, không phiên cuối tuần và không return
  tuyệt đối vượt 50%.
- 30 giá trị `daily_return` null là quan sát đầu tiên của mỗi ticker.
- 28 dòng Yahoo có OHLC bất hợp lý được đưa vào quarantine, không đi vào Silver.
- Số phiên khác nhau theo mã do ngày niêm yết/lịch dữ liệu Yahoo; ví dụ BCM bắt
  đầu 15/03/2021 và GVR bắt đầu 17/03/2020.

## 5. Timezone và point-in-time audit

Gold giữ Spark session ở UTC rồi chuyển `published_timestamp` sang
`Asia/Ho_Chi_Minh` đúng một lần. Sau khi mở rộng market về trước thời điểm news:

- 3.333 cặp news–ticker; 1.159 cặp đăng từ 15:00 trở đi.
- 0 trường hợp T0 đứng trước event date.
- 0 trường hợp anchor T−1 không đứng trước T0.
- Tin từ năm 2020 không còn bị ghép nhầm với phiên đầu năm 2023.

## 6. Event-window coverage

| Offset | Available | Tỷ lệ |
|---:|---:|---:|
| T−5 | 3.309/3.333 | 99,3% |
| T−3 đến T−1 | 3.310/3.333 | 99,3% |
| T0 | 3.333/3.333 | 100% |
| T+1 | 3.309/3.333 | 99,3% |
| T+2 | 3.282/3.333 | 98,5% |
| T+3 | 3.254/3.333 | 97,6% |
| T+5 | 3.252/3.333 | 97,6% |

EDA thô cho mean abnormal return khoảng +0,09% tại T0, +0,17% tại T+1 và
+0,31% tại T+5. Không nên diễn giải các số này là tác động của tin: nhiều bài
cùng sự kiện/ticker có thể tương quan, phân loại sentiment/event chưa có, và
VN30 equal-weight benchmark chưa điều chỉnh beta hay yếu tố ngành.

## 7. Storage local

| Layer | Parquet files | Dung lượng local |
|---|---:|---:|
| Silver News | 74 | 2.874,0 KiB |
| Silver Market | 83 | 6.654,8 KiB |
| Gold | 74 | 2.600,9 KiB |

## 8. Việc nên làm tiếp

1. Crawl full article hợp lệ và tăng mạnh coverage nội dung.
2. Chuẩn hóa publisher và review 1 cặp duplicate content.
3. Label thủ công ticker trên test set độc lập; review NER disagreement.
4. Gom các bài cùng sự kiện để tránh một sự kiện bị đếm nhiều lần.
5. Thêm event type/sentiment rồi xây baseline theo time-based split.
6. So sánh market-adjusted, sector-adjusted và beta-adjusted abnormal return.
