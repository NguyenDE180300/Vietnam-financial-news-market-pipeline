# Permutation Feature Importance — Full-content News–Market Models

Ngày chạy: 28/10/2026

## Phương pháp

Importance được tính bằng permutation trên validation và test đã giữ riêng theo
thời gian. Với mỗi feature, giá trị được xáo trộn giữa các hàng 30 lần, giữ
nguyên model và các feature còn lại. Importance là:

```text
Macro-F1 baseline - Macro-F1 sau khi permutation
```

Ví dụ một feature có ba giá trị `[0.01, -0.02, 0.005]`; sau khi tráo có thể
thành `[0.005, 0.01, -0.02]`. Cột vẫn tồn tại và các giá trị vẫn có phân phối
như cũ, nhưng mỗi giá trị không còn gắn với đúng mẫu. Sau đó dùng model đã
train để dự đoán lại. Đây **không phải xóa cột rồi train lại**: chỉ tráo dữ liệu
feature ở bước đánh giá, còn model và trọng số giữ nguyên.

Ví dụ Expanded `Fulltext + market + history`, T+5 test: baseline Macro-F1 là
0.5976. Khi tráo history embedding, Macro-F1 giảm trung bình 0.0784 qua 30
lần, tức còn khoảng 0.5192. Điểm 0.0784 không phải 7.84% accuracy và cũng
không chứng minh history gây ra mức tăng đó.

Giá trị dương nghĩa là model có ích từ feature đó trên split đang xét. Giá trị
âm nghĩa là permutation làm điểm tăng, có thể feature gây nhiễu/overfit ở split
này hoặc kết quả bị dao động do mẫu nhỏ. PhoBERT vector 768 chiều được
permuted nguyên khối; không xếp hạng riêng từng tọa độ embedding.

Ba model được kiểm tra:

| Run | Model |
|---|---|
| Fixed | Fulltext + market + history |
| Expanded | Fulltext + market |
| Expanded | Fulltext + market + history |

Fixed split: train đến 31/12/2024, validation năm 2025, test năm 2026.
Expanded split: train đến 31/12/2025, validation đến 30/04/2026, test sau
30/04/2026. Importance không dùng để fit hoặc chọn lại model.

## Kết quả nổi bật trên test

### Fixed — Fulltext + market + history

| Horizon | Baseline Macro-F1 | Importance cao nhất |
|---|---:|---|
| T+1 | 0.4362 | `days_since_last_history` +0.0022; `market_log1p_volume` +0.0009; `history_count` +0.0009 |
| T+3 | 0.5037 | `history_max_similarity` +0.0049; `market_log1p_volume` +0.0041; `market_volume_ratio_20` +0.0027 |
| T+5 | 0.5688 | `history_text_embedding` +0.0660; `current_text_embedding` +0.0336; `market_volatility_20` +0.0030 |

### Expanded — Fulltext + market

| Horizon | Baseline Macro-F1 | Importance cao nhất |
|---|---:|---|
| T+1 | 0.4574 | `market_return_1d` +0.0237; `market_intraday_range` +0.0099; `market_momentum_20` +0.0096 |
| T+3 | 0.5348 | `current_text_embedding` +0.0319; `market_open_gap` +0.0102; `market_momentum_5` +0.0077 |
| T+5 | 0.5467 | `current_text_embedding` +0.0542; `market_momentum_5` +0.0102; `market_momentum_20` +0.0100 |

### Expanded — Fulltext + market + history

| Horizon | Baseline Macro-F1 | Importance cao nhất |
|---|---:|---|
| T+1 | 0.4377 | `market_momentum_5` +0.0096; `market_momentum_20` +0.0069; `history_count` +0.0038 |
| T+3 | 0.5014 | `history_available_numeric` +0.0038; `history_mean_similarity` +0.0012; `market_volatility_20` +0.0003 |
| T+5 | 0.5976 | `history_text_embedding` +0.0784; `current_text_embedding` +0.0632; `market_momentum_5` +0.0147 |

`+0.0784` nghĩa là khi hoán đổi history embeddings giữa các rows, Macro-F1
trung bình giảm khoảng 0.0784 so với baseline trong 30 lần permutation.

## Diễn giải

1. **T+5 có tín hiệu text/history rõ nhất.** Trên expanded test, history
   embedding và current article embedding là hai nhóm quan trọng nhất; trên
   fixed test, cùng xu hướng xuất hiện ở T+5.
2. **T+1 thiên về market state hơn.** Với expanded Fulltext + market, return
   gần nhất là feature quan trọng nhất (+0.0237). Khi thêm history, current và
   history text embeddings có permutation drop âm ở T+1 (-0.0129 và -0.0347),
   cho thấy text chưa giúp tổng quát hóa ở holdout này.
3. **T+3 yếu/không ổn định khi thêm history.** Importance của hầu hết feature
   nhỏ so với độ lệch giữa các lần permutation; không nên kết luận nhóm nào có
   ảnh hưởng rõ ở horizon này.
4. **History scalar thường yếu hơn history embedding tại T+5.** Các biến
   `history_count`, similarity và days-since-last-history có đóng góp nhỏ hơn
   vector ngữ nghĩa, ngoại trừ một vài trường hợp T+1/T+3.
5. Kết quả phù hợp với ablation: history giúp T+5, nhưng không tạo cải thiện
   đồng đều cho T+1/T+3.

## Validation so với test

Feature importance đổi đáng kể giữa validation và test. Ví dụ current text
embedding có Macro-F1 drop +0.1142 ở Expanded T+1 validation nhưng -0.0526 ở
test; với history model là +0.1135 trên validation và -0.0129 trên test. Đây
là bằng chứng về độ nhạy theo thời gian/regime và test nhỏ, không phải lý do để
chọn feature theo test.

## Hạn chế khi diễn giải

- Permutation importance đo phụ thuộc dự đoán của model hiện tại, không đo quan
  hệ nhân quả.
- Xáo trộn phá vỡ tương quan tự nhiên giữa các feature; các feature tương quan
  có thể chia sẻ hoặc che lấp importance.
- Embedding được đánh giá ở cấp vector nguyên khối; không thể nói chiều PhoBERT
  nào tương ứng một khái niệm cụ thể.
- Expanded test chỉ có 329–344 rows/horizon; fixed test có 549–578 rows.
- Mean drop nhỏ hơn độ lệch chuẩn giữa permutations nên xem là chưa phân biệt
  được với nhiễu. Giá trị âm thường là dấu hiệu cần regularization/validation
  nhiều fold, không chứng minh feature có tác động ngược thật.
- Model là logistic regression và các kết quả phụ thuộc temporal split, label
  theo dấu raw return, cũng như similarity threshold lịch sử chưa hiệu chỉnh.

Không dùng bảng này để giải thích tác động kinh tế hay đưa ra quyết định giao
dịch. Cần xác nhận qua nhiều walk-forward folds và bootstrap theo event/ticker-date.

## Artifact

- Chi tiết mọi feature, validation/test, mean và standard deviation:
  `results/fulltext_model/feature_importance_permutation.json`
- Script tái lập:
  `news_system/spark_jobs/feature_importance_permutation.py`

Script cũng đối chiếu dự đoán tính từ coefficient matrix với Spark model cho
từng horizon; các dự đoán khớp trước khi tính permutation importance.
