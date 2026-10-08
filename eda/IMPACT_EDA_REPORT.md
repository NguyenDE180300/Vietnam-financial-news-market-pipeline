# Event–Market Impact Gold EDA

Ngày chạy: 08/10/2026. Dataset: `data_lake/gold/news_market_impact`.

## Quy mô và point-in-time quality

| Chỉ số | Giá trị |
|---|---:|
| Gold rows | 29.997 |
| News events | 3.003 |
| Event–ticker pairs | 3.333 |
| Ticker | 30 |
| Window key trùng | 0 |
| Tin sau 15:00 | 1.159 |
| T0 sai thời gian | 0 |
| Anchor T−1 sai | 0 |

## Event-window coverage

| Offset | Available | Tỷ lệ |
|---:|---:|---:|
| T−5 | 3.309/3.333 | 99,28% |
| T−3 đến T−1 | 3.310/3.333 | 99,31% |
| T0 | 3.333/3.333 | 100% |
| T+1 | 3.309/3.333 | 99,28% |
| T+2 | 3.282/3.333 | 98,47% |
| T+3 | 3.254/3.333 | 97,63% |
| T+5 | 3.252/3.333 | 97,57% |

## Abnormal return mô tả

Mean cumulative abnormal return là khoảng +0,089% tại T0, +0,168% tại T+1,
+0,106% tại T+3 và +0,304% tại T+5. Median tại T+5 lại là −0,160%, cho thấy
phân phối lệch và mean có thể bị một nhóm return lớn kéo lên.

Tin trước 15:00 có mean T+5 khoảng +0,285%; tin sau 15:00 khoảng +0,340%.
Đây chỉ là mô tả, chưa phải bằng chứng tin sau giờ đóng cửa tạo tác động lớn hơn.

Kết quả theo ticker rất mất cân bằng: FPT có 1.448 quan sát T+5, ACB có 930,
trong khi nhiều mã chỉ có 20–60. Mean theo ticker vì thế không thể so sánh trực
tiếp nếu chưa cân bằng mẫu và gom các bài cùng sự kiện.

## Kết luận Impact

Point-in-time join và coverage đã tốt. Chưa nên gọi các mean là tác động nhân
quả do ticker/source imbalance, các bài trùng sự kiện, market outlier và chưa có
event type/sentiment. Bước tiếp theo là event clustering, outlier audit và
time-based evaluation.
