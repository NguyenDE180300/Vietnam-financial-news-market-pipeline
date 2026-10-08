# News Silver EDA

Ngày chạy: 08/10/2026. Dataset: `data_lake/silver/news`.

## Quy mô và chất lượng

| Chỉ số | Giá trị |
|---|---:|
| Bài viết | 3.003 |
| Event ID duy nhất | 3.003 |
| URL trùng | 0 |
| Content hash trùng | 1 |
| Dòng invalid | 0 |
| Có ticker VN30 | 3.003 (100%) |
| Full article | 16 (0,53%) |
| Metadata-only | 2.987 (99,47%) |

Dữ liệu phủ từ 01/01/2020 đến 06/10/2026. Silver hiện đúng vai trò
ticker-qualified: không còn bài thiếu ticker VN30.

## Content

Biểu đồ content đã được tách làm hai phần thay vì dồn giá trị 0 vào histogram:

- Tỷ lệ metadata-only so với full article.
- Phân bố độ dài chỉ trên 16 full article.

Full article dài trung vị 3.344 ký tự; ngắn nhất 1.307 và dài nhất 5.411 ký tự.
Coverage 0,53% vẫn quá thấp để huấn luyện sentiment/event model trên toàn văn.

## Nguồn và ticker

Có 176 tên nguồn. Các nguồn lớn gồm CafeF (456), FPT Shop (323), Chúng Ta
(317), Dân Trí (122) và Thanh Niên (92). Tên nguồn chưa canonical hoàn toàn.

| Nhóm extraction | Bài |
|---|---:|
| Rule và NER đồng ý | 1.721 |
| Rule-only | 1.228 |
| NER-only | 10 |
| Partial disagreement | 44 |

Ticker phân bố rất lệch: FPT có 1.451 lượt nhắc và ACB có 953, trong khi POW
chỉ có 1. Vì vậy không được random split trực tiếp cho mô hình; cần stratify theo
ticker, thời gian và nguồn, đồng thời review 54 bài NER-only/disagreement.

## Kết luận News

News Silver sạch về khóa và ticker gate, nhưng chưa cân bằng và gần như toàn bộ
vẫn là metadata. Ưu tiên tiếp theo là crawl full article, canonical publisher và
review duplicate content trước NLP.
