# Event–Market Impact Gold EDA

Ngày chạy: 10/10/2026. Dataset: `data_lake/gold/news_market_impact`.

## Quy mô và point-in-time quality

| Chỉ số | Giá trị |
|---|---:|
| Gold rows | 44.199 |
| News events | 2.879 |
| Event–ticker pairs | 4.911 |
| Ticker | 30 |
| Window key trùng | 0 |
| Tin sau 15:00 | 1.728 |
| T0 sai thời gian | 0 |
| Anchor T−1 sai | 0 |

## Event-window coverage

| Offset | Available | Tỷ lệ |
|---:|---:|---:|
| T−5 | 4.886/4.911 | 99,49% |
| T−3 đến T−1 | 4.887/4.911 | 99,51% |
| T0 | 4.911/4.911 | 100% |
| T+1 | 4.887/4.911 | 99,51% |
| T+2 | 4.860/4.911 | 98,96% |
| T+3 | 4.832/4.911 | 98,39% |
| T+5 | 4.830/4.911 | 98,35% |

## Abnormal return mô tả

Mean cumulative abnormal return là khoảng +0,020% tại T0, +0,069% tại T+1,
+0,063% tại T+3 và +0,079% tại T+5. Median tại T+5 lại là −0,287%, cho thấy
phân phối lệch và mean có thể bị một nhóm return lớn kéo lên.

Tin trước 15:00 có mean T+5 khoảng +0,079%; tin sau 15:00 khoảng +0,080%.
Đây chỉ là mô tả, chưa phải bằng chứng tin sau giờ đóng cửa tạo tác động lớn hơn.

Kết quả theo ticker vẫn mất cân bằng mạnh, nên mean theo ticker không thể so
sánh trực tiếp nếu chưa cân bằng mẫu và gom các bài cùng sự kiện.

## Kết luận Impact

Point-in-time join và coverage đã tốt. Chưa nên gọi các mean là tác động nhân
quả do ticker/source imbalance, các bài trùng sự kiện, market outlier và chưa có
event type/sentiment. Bước tiếp theo là event clustering, outlier audit và
time-based evaluation.
