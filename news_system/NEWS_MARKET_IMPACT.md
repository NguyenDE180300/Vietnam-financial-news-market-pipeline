# News → Market Impact Baseline

## Hệ thống đang đo gì?

Với mỗi cặp `(news, ticker)`, hệ thống lấy giá đóng cửa cuối cùng **đã biết** tại
thời điểm tin được đăng, sau đó đo biến động sau 1, 3 và 5 phiên giao dịch.

```text
raw return      = stock target close / stock anchor close - 1
benchmark return = VN-Index target / VN-Index anchor - 1
abnormal return = raw return - benchmark return
```

Nhãn mặc định:

- `positive`: abnormal return từ +2% trở lên.
- `negative`: abnormal return từ -2% trở xuống.
- `neutral`: nằm giữa hai ngưỡng trên.

## Chống look-ahead

- Tin đăng trước 15:00 giờ Việt Nam không được dùng close cùng ngày làm giá gốc.
- Tin đăng sau 15:00 có thể dùng close cùng ngày vì giá này đã hình thành.
- Horizon là phiên giao dịch thực tế, không phải ngày lịch.
- Giá cổ phiếu và VN-Index đều lấy từ VCI qua `vnstock` để tránh trộn nguồn.
- Dùng chuỗi giá đã xử lý corporate action của nguồn; kiểm tra ACB ngày
  23–26/05/2025 cho thấy return đúng khoảng -0,48%, thay vì biến động giả
  -15,85% từ Yahoo.

## Chạy

```bash
python -m news_system.news_market_impact \
  --db news_system.db \
  --sync-market \
  --request-delay 3.2 \
  --output data/news_market_impacts.csv
```

Job có thể resume: ticker đã đủ coverage trong `market_bars` sẽ được dùng từ
cache. Delay mặc định giúp tuân thủ rate limit của vnstock community.

## Output

- CSV: `data/news_market_impacts.csv`
- Event-window CSV: `data/news_market_event_windows.csv`
- SQLite: bảng `news_market_impacts`
- SQLite event window: bảng `news_market_event_windows`
- Streamlit: phần `News → Market impact`, chọn T+1/T+3/T+5.

Event-window dataset lưu riêng các phiên `T-5, T-3, T-2, T-1, T0, T+1,
T+2, T+3, T+5`. `T0` là phiên đầu tiên thị trường có thể phản ứng: tin trước
15:00 dùng phiên cùng ngày; tin sau 15:00, cuối tuần hoặc ngày nghỉ dùng phiên
giao dịch kế tiếp. Mỗi dòng lưu return riêng của phiên, return VN-Index và
abnormal return.

## Giới hạn diễn giải

Abnormal return trong cửa sổ sau tin là **tương quan**, không tự động chứng minh
tin tức gây ra biến động. Nhiều bài có thể nói về cùng sự kiện và nhiều sự kiện
khác có thể xuất hiện trong cùng cửa sổ. Trước khi train NLP prediction nên:

1. Gom các bài trùng/sát nhau thành một event.
2. Loại tin đăng lại sau khi thị trường đã phản ứng.
3. Kiểm soát overlap giữa event windows.
4. Split train/test theo thời gian, không random theo bài.
