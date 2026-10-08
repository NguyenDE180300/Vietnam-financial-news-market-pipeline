# Event–Market Impact Gold EDA

Ngày chạy: 08/10/2026. Dataset: `data_lake/gold/news_market_impact`.

## Quy mô và point-in-time quality

| Chỉ số | Giá trị |
|---|---:|
| Gold rows | 39.789 |
| News events | 3.033 |
| Event–ticker pairs | 4.421 |
| Ticker | 30 |
| Window key trùng | 0 |
| Tin sau 15:00 | 1.543 |
| T0 sai thời gian | 0 |
| Anchor T−1 sai | 0 |

## Event-window coverage

| Offset | Available | Tỷ lệ |
|---:|---:|---:|
| T−5 | 4.397/4.421 | 99,46% |
| T−3 đến T−1 | 4.398/4.421 | 99,48% |
| T0 | 4.421/4.421 | 100% |
| T+1 | 4.397/4.421 | 99,46% |
| T+2 | 4.370/4.421 | 98,85% |
| T+3 | 4.342/4.421 | 98,21% |
| T+5 | 4.340/4.421 | 98,17% |

## Abnormal return mô tả

Mean cumulative abnormal return là khoảng +0,040% tại T0, +0,085% tại T+1,
+0,020% tại T+3 và +0,154% tại T+5. Median tại T+5 lại là −0,210%, cho thấy
phân phối lệch và mean có thể bị một nhóm return lớn kéo lên.

Tin trước 15:00 có mean T+5 khoảng +0,160%; tin sau 15:00 khoảng +0,143%.
Đây chỉ là mô tả, chưa phải bằng chứng tin sau giờ đóng cửa tạo tác động lớn hơn.

Kết quả theo ticker vẫn mất cân bằng mạnh, nên mean theo ticker không thể so
sánh trực tiếp nếu chưa cân bằng mẫu và gom các bài cùng sự kiện.

## Kết luận Impact

Point-in-time join và coverage đã tốt. Chưa nên gọi các mean là tác động nhân
quả do ticker/source imbalance, các bài trùng sự kiện, market outlier và chưa có
event type/sentiment. Bước tiếp theo là event clustering, outlier audit và
time-based evaluation.
