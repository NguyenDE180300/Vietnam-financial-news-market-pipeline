# Full-content model results

Thư mục này chứa metrics đã làm sạch của thí nghiệm dự đoán hướng raw return
cho các horizon T+1, T+3 và T+5. Không có raw news, article URL, prediction-level
rows, database, Data Lake, embedding cache, model weights hoặc credential.

## Files

```text
hierarchical_phobert_metrics.json
fixed/
├── fulltext_only_metrics.json
├── fulltext_market_metrics.json
└── fulltext_market_history_metrics.json
expanded/
├── fulltext_only_metrics.json
├── fulltext_market_metrics.json
└── fulltext_market_history_metrics.json
```

`fixed` sử dụng train đến 31/12/2024, validation trong năm 2025 và test sau
31/12/2025. `expanded` sử dụng train đến 31/12/2025, validation đến 30/04/2026
và test sau 30/04/2026. Expanded là một temporal holdout với train window mở
rộng, không phải walk-forward evaluation nhiều folds.

Label order của binary experiment:

```text
0 = decrease: raw return < 0
1 = increase: raw return > 0
```

Return bằng 0 hoặc target không khả dụng bị mask. Neutral bands của Gold
three-class label contract không được dùng trong binary experiment này.

Metric chính là Macro-F1. Báo cáo và diễn giải đầy đủ nằm tại
`news_system/FULLTEXT_MODEL_EVALUATION_REPORT.md`.

Permutation importance theo PhoBERT embedding, market và history groups nằm tại
`FEATURE_IMPORTANCE_REPORT.md`; dữ liệu đầy đủ cho 30 lần permutation trên mỗi
feature nằm trong `feature_importance_permutation.json`.

## Summary

| Setting | Feature set | T+1 Macro-F1 | T+3 Macro-F1 | T+5 Macro-F1 | Mean |
|---|---|---:|---:|---:|---:|
| Fixed | Fulltext-only | 0.4684 | 0.5208 | 0.5156 | 0.5016 |
| Fixed | Fulltext + market | 0.4621 | 0.5318 | 0.4965 | 0.4968 |
| Fixed | Fulltext + market + history | 0.4362 | 0.5037 | 0.5688 | 0.5029 |
| Expanded | Fulltext-only | 0.4353 | 0.5170 | 0.5377 | 0.4967 |
| Expanded | Fulltext + market | 0.4574 | 0.5348 | 0.5467 | 0.5130 |
| Expanded | Fulltext + market + history | 0.4377 | 0.5014 | 0.5976 | 0.5122 |

Các JSON giữ objective history, class counts, split counts, per-split metrics,
per-class F1 và confusion matrices do training pipeline xuất ra. Đường dẫn máy
local trong trường `input` đã được thay bằng đường dẫn logic của dataset.
