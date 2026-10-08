# Market Silver EDA

Ngày chạy: 08/10/2026. Dataset: `data_lake/silver/market_daily`.

## Quy mô và chất lượng

| Chỉ số | Giá trị |
|---|---:|
| Dòng OHLCV | 52.142 |
| Ticker | 30 |
| Ngày giao dịch | 1.788 |
| Coverage | 02/12/2019–07/10/2026 |
| Khóa trùng | 0 |
| Dòng invalid | 0 |
| Phiên cuối tuần | 0 |
| Return null | 30 |
| `abs(daily_return) > 10%` | 46 |

30 return null là quan sát đầu tiên của từng ticker. Các mã niêm yết/chuyển sàn
muộn có coverage ngắn hơn: BCM 1.443 phiên, SSB 1.445, ACB 1.519 và VIB 1.542.

## Return và volatility

Daily return trung bình khoảng 0,064%, độ lệch chuẩn 2,157%. P1 khoảng −6,87%,
P99 khoảng +6,87%. Giá trị nhỏ nhất −49,54% và lớn nhất +31,25% cần audit riêng
vì có thể liên quan corporate action, điều chỉnh giá hoặc lỗi nguồn.

Các mã có volatility mẫu cao nhất là GVR (2,84%), SHB (2,65%) và SSI (2,59%).
VNM thấp nhất trong tập hiện tại, khoảng 1,54%.

## Kết luận Market

Market Silver đủ dài và sạch để làm event study. Tuy nhiên 46 phiên vượt 10%
phải được đối chiếu corporate action trước khi dùng cho nhãn tăng/giảm hoặc mô
hình dự đoán. Correlation heatmap chỉ nên đọc sau khi xử lý các outlier này.
