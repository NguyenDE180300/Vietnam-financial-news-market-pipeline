# News Silver EDA

Ngày chạy: 10/10/2026. Dataset: `data_lake/silver/news`.

## Quy mô và chất lượng

| Chỉ số | Giá trị |
|---|---:|
| Bài viết | 2.879 |
| Event ID duy nhất | 2.879 |
| URL trùng | 0 |
| Content hash trùng | 1 |
| Dòng invalid | 0 |
| Có ticker VN30 | 2.879 (100%) |
| Full article | 2.707 (94,03%) |
| Metadata-only | 172 (5,97%) |

Dữ liệu phủ từ 01/01/2020 đến 06/10/2026. Silver hiện đúng vai trò
ticker-qualified: không còn bài thiếu ticker VN30.

## Content

Biểu đồ content đã được tách làm hai phần thay vì dồn giá trị 0 vào histogram:

- Tỷ lệ metadata-only so với full article.
- Phân bố độ dài chỉ trên 2.707 full article.

Full article dài trung vị 3.093 ký tự; ngắn nhất 241 và dài nhất 35.666 ký tự.
Coverage đã tăng lên 94,03%; 172 bài còn lại chỉ có metadata.

## Nguồn và ticker

Có 176 tên nguồn. Các nguồn lớn gồm CafeF (472), Chúng Ta (319), FPT Shop
(144), Dân Trí (122) và Thanh Niên (92). Tên nguồn chưa canonical hoàn toàn.

| Nhóm extraction | Bài |
|---|---:|
| Rule và NER đồng ý | 1.980 |
| Rule-only | 673 |
| NER-only | 10 |
| Partial disagreement | 216 |

Ticker phân bố rất lệch: FPT có 1.316 lượt nhắc và ACB có 1.075. Vì vậy không
được random split trực tiếp cho mô hình; cần stratify theo ticker, thời gian và
nguồn, đồng thời review 226 bài NER-only/disagreement.

## Kết luận News

News Silver sạch về khóa và ticker gate, full article đã đạt 94,03% nhưng dữ
liệu vẫn mất cân bằng. Ưu tiên tiếp theo là canonical publisher, review các bài
Rule/NER bất đồng và kiểm tra chất lượng nội dung trước NLP.
