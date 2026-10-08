# News Silver EDA

Ngày chạy: 08/10/2026. Dataset: `data_lake/silver/news`.

## Quy mô và chất lượng

| Chỉ số | Giá trị |
|---|---:|
| Bài viết | 3.033 |
| Event ID duy nhất | 3.033 |
| URL trùng | 0 |
| Content hash trùng | 1 |
| Dòng invalid | 0 |
| Có ticker VN30 | 3.033 (100%) |
| Full article | 1.155 (38,08%) |
| Metadata-only | 1.878 (61,92%) |

Dữ liệu phủ từ 01/01/2020 đến 06/10/2026. Silver hiện đúng vai trò
ticker-qualified: không còn bài thiếu ticker VN30.

## Content

Biểu đồ content đã được tách làm hai phần thay vì dồn giá trị 0 vào histogram:

- Tỷ lệ metadata-only so với full article.
- Phân bố độ dài chỉ trên 1.155 full article.

Full article dài trung vị 3.097 ký tự; ngắn nhất 241 và dài nhất 19.969 ký tự.
Coverage đã tăng lên 38,08%, đủ để bắt đầu audit và tạo tập huấn luyện thử,
nhưng vẫn cần tiếp tục crawl để giảm thiên lệch giữa bài có và chưa có toàn văn.

## Nguồn và ticker

Có 176 tên nguồn. Các nguồn lớn gồm CafeF (464), FPT Shop (323), Chúng Ta
(317), Dân Trí (122) và Thanh Niên (92). Tên nguồn chưa canonical hoàn toàn.

| Nhóm extraction | Bài |
|---|---:|
| Rule và NER đồng ý | 1.680 |
| Rule-only | 1.214 |
| NER-only | 10 |
| Partial disagreement | 129 |

Ticker phân bố rất lệch: FPT có 1.474 lượt nhắc và ACB có 1.042. Vì vậy không
được random split trực tiếp cho mô hình; cần stratify theo ticker, thời gian và
nguồn, đồng thời review 139 bài NER-only/disagreement.

## Kết luận News

News Silver sạch về khóa và ticker gate, full article đã tăng mạnh nhưng dữ liệu
vẫn mất cân bằng. Ưu tiên tiếp theo là hoàn tất crawl, canonical publisher và
review duplicate content trước NLP.
