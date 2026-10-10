# Vietnam Financial News–Market EDA Report

Ngày chạy: 10/10/2026. Báo cáo được tạo sau khi đồng bộ và chạy toàn bộ
`COMBINED_EDA.ipynb` theo thứ tự News Silver → Market Silver → Event–Market
Gold.

## 1. News Silver

| Chỉ số | Giá trị |
|---|---:|
| Bài viết | 2.879 |
| Event ID duy nhất | 2.879 |
| Có ticker VN30 | 2.879 (100%) |
| Full article | 2.707 (94,03%) |
| Metadata-only | 172 (5,97%) |
| URL trùng | 0 |
| Content hash trùng | 1 |
| Dòng invalid | 0 |

Dữ liệu tin tức phủ từ 01/01/2020 đến 06/10/2026. Full article có độ dài
trung vị 3.093 ký tự, nhỏ nhất 241 và lớn nhất 35.666 ký tự. Coverage toàn văn
đã đạt 94,03%; 172 bài còn lại chỉ có metadata.

Phân bố ticker mất cân bằng: FPT có 1.316 lượt nhắc và ACB có 1.075. Rule và
NER đồng ý hoàn toàn ở 1.980 bài; có 10 bài NER-only và 216 bài disagreement
cần ưu tiên review thủ công.

## 2. Market Silver

| Chỉ số | Giá trị |
|---|---:|
| Dòng OHLCV | 52.142 |
| Ticker | 30 |
| Phiên giao dịch | 1.788 |
| Khoảng thời gian | 02/12/2019–07/10/2026 |
| Khóa trùng | 0 |
| Dòng invalid | 0 |
| Phiên cuối tuần | 0 |
| Return null | 30 |
| `abs(daily_return) > 10%` | 46 |

Daily return trung bình là 0,064%, độ lệch chuẩn 2,157%. P1 khoảng −6,869%
và P99 khoảng +6,875%. GVR, SHB và SSI có volatility mẫu cao nhất; VNM thấp
nhất. Các return tuyệt đối trên 10% cần được đối chiếu corporate action trước
khi dùng làm nhãn hoặc huấn luyện mô hình.

## 3. Event–Market Gold

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

| Relative session | Market coverage |
|---:|---:|
| T−5 | 99,49% |
| T−3 đến T−1 | 99,51% |
| T0 | 100% |
| T+1 | 99,51% |
| T+2 | 98,96% |
| T+3 | 98,39% |
| T+5 | 98,35% |

Mean cumulative abnormal return lần lượt khoảng +0,020% tại T0, +0,069% tại
T+1, +0,063% tại T+3 và +0,079% tại T+5. Median T+5 là −0,287%, cho thấy
phân phối lệch và mean bị ảnh hưởng bởi một số quan sát lớn. Đây là thống kê mô
tả, không phải bằng chứng quan hệ nhân quả giữa tin và biến động giá.

## 4. Kết luận và bước tiếp theo

Pipeline đạt chất lượng kỹ thuật tốt: khóa không trùng, ticker gate đầy đủ,
point-in-time join không vi phạm thời gian và market coverage trên 98% ở mọi
offset. Rủi ro chính hiện tại là mất cân bằng ticker/nguồn, 5,97% bài chưa có
toàn văn, market outlier và nhiều bài có thể cùng mô tả một sự kiện.

Trước khi xây mô hình dự đoán, nên hoàn tất crawl full article, canonical hóa
nguồn, audit corporate action, gom bài trùng sự kiện và chia train/test theo
thời gian thay vì random split.
