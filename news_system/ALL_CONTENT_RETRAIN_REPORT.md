# All-content Hierarchical PhoBERT retrain

Ngày chạy: 21/10/2026

## Thay đổi

- Bỏ hoàn toàn giới hạn 8 content chunks.
- Đọc content đến hết bài; không truncate theo số chunk.
- Mỗi chunk dài 192 tokens, overlap 32 tokens.
- Variable-length cache và dynamic batch padding.
- Segment encoding chạy theo micro-batch 32 để kiểm soát VRAM.
- Sinusoidal position encoding thay learned fixed-size position table.

Thống kê 2,948 articles:

| Statistic | Số segment/article, gồm title và summary |
|---|---:|
| Minimum | 3 |
| Median | 6 |
| P95 | 14 |
| Maximum | 46 |

Không article nào bị cắt theo số chunk.

## Multi-task attention model

Best checkpoint là epoch 1; early stopping sau epoch 5. Mean validation macro-F1 tốt nhất là 0.5049.

### Test

| Horizon | Accuracy | Balanced accuracy | Macro-F1 | ROC-AUC | Majority |
|---|---:|---:|---:|---:|---:|
| T+1 | .5446 | .5468 | .5415 | .5478 | .5865 |
| T+3 | .5571 | .5131 | .5034 | .5259 | .5952 |
| T+5 | .4809 | .5129 | .4761 | .5217 | .5868 |

Mean test macro-F1: 0.5070. Không head nào vượt majority accuracy.

## Attention audit

| Split | Title | Summary | Content |
|---|---:|---:|---:|
| Validation | .114% | .012% | **99.874%** |
| Test | .100% | .010% | **99.889%** |

Khác bản giới hạn 8 chunks, model mới thực sự dựa gần như hoàn toàn vào content. Tuy nhiên attention lại collapse theo hướng ngược: gần như bỏ title và summary. Vì vậy cần two-stage pooling hoặc field dropout trong vòng sau.

## Replaced feature ablations

Ba phiên bản cũ dựa vào title + summary đã được train lại bằng article embedding toàn-content. Market-only không thay đổi vì không dùng text.

Mỗi ô là `accuracy / macro-F1`.

### Validation

| Feature set | T+1 | T+3 | T+5 |
|---|---:|---:|---:|
| Fulltext-only | .4554 / .4553 | .5090 / .5074 | .4902 / .4885 |
| Fulltext + market | .4530 / .4530 | .5068 / .5056 | .4955 / .4942 |
| Fulltext + market + fulltext history | **.5036 / .4989** | **.5166 / .5145** | **.5159 / .5149** |

### Test

| Feature set | T+1 | T+3 | T+5 |
|---|---:|---:|---:|
| Fulltext-only | **.4991 / .4684** | .5363 / .5208 | .5156 / .5156 |
| Fulltext + market | .4936 / .4621 | **.5484 / .5318** | .4965 / .4965 |
| Fulltext + market + fulltext history | .4645 / .4362 | .5277 / .5037 | **.5747 / .5688** |

Mean test macro-F1:

| Feature set | Mean macro-F1 |
|---|---:|
| Fulltext-only | .5016 |
| Fulltext + market | .4968 |
| Fulltext + market + fulltext history | **.5029** |

Title + summary V2 cũ vẫn cao hơn với mean test macro-F1 0.5237. Vì vậy thay input bằng toàn content chưa cải thiện tổng thể, dù history toàn-content giúp rõ ở T+5.

## Rolling retrain (train đến 2025)

Để thay luôn các phiên bản rolling chỉ dùng `title + summary`, ba feature set đã được train lại với cùng full-content embedding. Split giữ nguyên để so sánh công bằng: train đến 31/12/2025, validation đến 30/04/2026, test sau 30/04/2026. Số hàng thô tương ứng là 4,479 / 242 / 347; từng horizon có thể ít hơn vì thiếu market label.

Mỗi ô là `accuracy / macro-F1`.

### Validation

| Feature set | T+1 | T+3 | T+5 |
|---|---:|---:|---:|
| Fulltext-only | .6091 / .6070 | .4658 / .4656 | .4085 / .4083 |
| Fulltext + market | .5955 / .5924 | .4829 / .4821 | .4255 / .4240 |
| Fulltext + market + fulltext history | **.6136 / .6094** | **.5214 / .5144** | **.5277 / .5255** |

### Test

| Feature set | T+1 | T+3 | T+5 |
|---|---:|---:|---:|
| Fulltext-only | .4468 / .4353 | .5174 / .5170 | .5396 / .5377 |
| Fulltext + market | **.4650 / .4574** | **.5349 / .5348** | .5484 / .5467 |
| Fulltext + market + fulltext history | .4377 / .4377 | .5029 / .5014 | **.5982 / .5976** |

| Feature set | Mean test macro-F1 | Title + summary rolling cũ |
|---|---:|---:|
| Fulltext-only | .4967 | .4596 |
| Fulltext + market | **.5130** | .4645 |
| Fulltext + market + fulltext history | .5122 | .5000 |

Trên rolling split, cả ba bản full-content đều vượt bản `title + summary` tương ứng theo mean test macro-F1. Tuy nhiên độ lệch validation–test, đặc biệt T+1, vẫn lớn; đây chưa phải bằng chứng rằng model ổn định qua mọi market regime.

## Fulltext history

- Current và historical article vectors đều được sinh từ all-content attention model.
- Same ticker, strictly previous calendar day, lookback 14 days, top 5.
- Threshold cosine tạm giữ 0.30–0.85.
- History coverage: 3,815/5,068 = 75.28%.

Coverage tăng mạnh cho thấy cosine distribution của article embedding mới khác embedding cũ. Threshold này chưa được hiệu chỉnh lại, nên kết quả history hiện là exploratory.

## Artifacts

- Cache: `data/hierarchical_phobert_all_content_cache_v2.pt`
- Multi-task model: `models/hierarchical_phobert_all_content_binary_v2`
- Fulltext Gold: `data_lake/gold/training_targets_fulltext_attention_v1`
- Fulltext-only: `models/ablation_binary_fulltext_text_only_v1`
- Fulltext + market: `models/ablation_binary_fulltext_text_market_v1`
- Fulltext + market + history: `models/ablation_binary_fulltext_text_market_history_v1`
- Rolling fulltext-only: `models/ablation_binary_fulltext_rolling_v2_text_only`
- Rolling fulltext + market: `models/ablation_binary_fulltext_rolling_v2_text_market`
- Rolling fulltext + market + history: `models/ablation_binary_fulltext_rolling_v2_text_market_history`

Các artifact `title + summary` cũ được giữ lại để audit/so sánh, nhưng không còn là phiên bản đề xuất. Market-only vẫn được giữ nguyên vì không chứa nhánh text.

