1# Báo cáo tổng hợp hệ thống dự đoán tác động News–Market

**Project:** Vietnam Financial News–Market Pipeline

**Ngày tổng hợp:** 22/10/2026

**Phạm vi:** xử lý dữ liệu, thiết kế mô hình, training protocol, kết quả và phân tích

## Tóm tắt

Project xây dựng pipeline từ thu thập tin tài chính Việt Nam đến dự đoán hướng
biến động giá của cổ phiếu VN30 tại `T+1`, `T+3` và `T+5` phiên giao dịch. Đơn
vị dự đoán là một cặp `(event_id, ticker)`: một bài báo nhắc nhiều mã sẽ tạo
nhiều mẫu với market features và labels riêng cho từng mã.

Dataset full-content cuối gồm 2.948 bài báo, 5.068 cặp event–ticker, đủ 30 mã
VN30 và trải từ 01/01/2020 đến 24/09/2026. Nhánh text dùng financial PhoBERT
để encode title, summary và toàn bộ content; các segment được gộp bằng trainable
additive attention. Vector bài báo sau đó được kết hợp với market và historical-
news features trong các thí nghiệm Spark Logistic Regression.

Kết quả tốt nhất theo trung bình ba horizon trên expanded temporal test là
`Fulltext + market`, đạt mean Macro-F1 0,5130. Kết quả đơn lẻ tốt nhất là
`Fulltext + market + history` tại T+5, với accuracy 0,5982 và Macro-F1 0,5976.
History cải thiện T+5 ở cả hai temporal settings nhưng không cải thiện T+1.
Hệ thống hiện là research prototype, chưa phải mô hình phục vụ giao dịch.

---

## 1. Bài toán nghiên cứu

### 1.1 Mục tiêu

Từ một bài báo tài chính và ticker được nhắc trong bài, hệ thống dự đoán hướng
raw stock return tại:

- `T+1`: close của phiên giao dịch thứ nhất sau ngày event;
- `T+3`: close của phiên giao dịch thứ ba sau ngày event;
- `T+5`: close của phiên giao dịch thứ năm sau ngày event.

Thí nghiệm model hiện tại là binary direction classification:

```text
0 = decrease: stock_return < 0
1 = increase: stock_return > 0
```

Return bằng đúng 0 hoặc target không khả dụng bị mask khỏi head tương ứng.
Model không dự đoán giá tuyệt đối và kết quả không phải khuyến nghị giao dịch.

### 1.2 Đơn vị huấn luyện

Đơn vị huấn luyện là `(event_id, ticker)`. Ví dụ một bài nhắc FPT và MWG tạo:

```text
(event_001, FPT)
(event_001, MWG)
```

Hai mẫu dùng chung nội dung bài nhưng có market features và labels riêng. Vì
vậy event–ticker rows không hoàn toàn độc lập về mặt thống kê.

### 1.3 Phạm vi

Model cuối sử dụng title, summary, full article, market features có thể quan
sát tại prediction time và historical news trong một ablation riêng. Ảnh đã
được tải nhưng chưa tham gia model. T0 có trong Gold dataset nhưng không được
dùng làm prediction head trong thí nghiệm cuối.

---

## 2. Kiến trúc dữ liệu

```text
Historical Google News RSS       Realtime RSS
            |                         |
            +-------- Collector ------+
                         |
                  Full-article crawl
                         |
                 Bronze raw events
                         |
             Spark cleaning + normalize
                         |
                   Silver News
                         |
       Rule-based ticker + CRF NER + VN30 filter
                         |
                  Event–ticker pairs
                         |
Yahoo Finance -> Silver Market -> point-in-time join
                         |
              Gold training targets
                         |
       Hierarchical PhoBERT + Spark models
```

### 2.1 Thu thập

- **Historical backfill:** Google News RSS được query theo ticker và khoảng
  thời gian, sau đó redirect được giải mã về URL publisher.
- **Incremental/realtime:** CafeF Stock, CafeF Company, CafeF Finance,
  VnExpress Business và VnEconomy Stock.

RSS dùng để phát hiện bài và lấy metadata. Hệ thống truy cập URL publisher để
lấy full article; RSS không được xem là nguồn toàn văn hoàn chỉnh.

### 2.2 Medallion layers

| Layer | Vai trò |
|---|---|
| Bronze | Payload gần nguồn, ingest timestamp và crawl metadata |
| Silver News | Text/URL/time/ticker chuẩn hóa và deduplication |
| Silver Market | Daily OHLCV và feature quá khứ |
| Gold | Một dòng/event–ticker, point-in-time features, targets và masks |

Kafka nhận tin mới theo luồng; Spark xử lý streaming/batch từ Bronze sang
Silver và materialize Gold. Local filesystem lake phục vụ phát triển; kiến
trúc có thể publish versioned runs lên ADLS.

---

## 3. Xử lý dữ liệu News

### 3.1 Chuẩn hóa và quality gate

1. Parse RSS và chuẩn hóa schema giữa nguồn.
2. Resolve/canonicalize URL để giảm duplicate do redirect/tracking query.
3. Chuẩn hóa Unicode, HTML, khoảng trắng và ký tự rác.
4. Chuẩn hóa publication time sang UTC và `Asia/Ho_Chi_Minh`.
5. Crawl full article từ publisher.
6. Tạo `content_hash`; deduplicate theo URL/content/event identity.
7. Loại article thiếu content hoặc content dưới 200 ký tự.
8. Chỉ giữ event có ticker VN30 sau extraction.

Các cột cốt lõi:

```text
event_id, title, summary, content, news_source,
url, canonical_url, content_hash, content_length,
prediction_time, event_date, published_precision,
published_before_close, published_after_close
```

### 3.2 Ticker extraction

```text
Rule-based alias/regex -----+
                            +--> union --> entity linking --> VN30 whitelist
CRF NER --------------------+
```

Rule-based sử dụng mã, tên doanh nghiệp, tên viết tắt và alias. CRF NER tìm
entity spans bị rule-based bỏ sót; entity linker ánh xạ entity text về mã.
Kết quả cuối bị giới hạn trong VN30. Dataset ticker NER trả lời “bài nhắc mã
nào”, khác với market label trả lời “giá mã đó tăng hay giảm sau bài”.

### 3.3 Dataset cuối

| Thống kê | Giá trị |
|---|---:|
| Unique articles | 2.948 |
| Event–ticker pairs | 5.068 |
| VN30 tickers | 30 |
| News sources | 177 |
| Date range | 01/01/2020–24/09/2026 |
| Rows có full article | 5.068/5.068 |
| Minimum content length | 241 ký tự |
| Median content length | 3.142 ký tự |
| Mean content length | 3.864 ký tự |
| P95 content length | 8.972 ký tự |
| Maximum content length | 35.666 ký tự |

Một article trung bình tạo 1,72 event–ticker pairs; median là một và maximum
là 22. Dependence này cần được xét khi bootstrap/kiểm định.

### 3.4 Ảnh

Ảnh được tải riêng, kiểm tra HTTP/MIME, deduplicate bằng SHA-256 và quản lý
retry trong image manifest. Ảnh chưa được encode bằng CLIP/ViT và không tham
gia kết quả trong báo cáo này; model hiện là text–market, chưa phải multimodal.

---

## 4. Market processing và target

### 4.1 Market data và observable anchor

Market data là daily OHLCV của 30 mã VN30 từ Yahoo Finance, có điều chỉnh
split/dividend. Anchor được chọn để chỉ dùng dữ liệu đã quan sát được:

- Tin chính xác, trước close: close phiên trước.
- Tin chính xác, sau close: close cùng ngày nếu là ngày giao dịch.
- Tin cuối tuần/ngày nghỉ hoặc thời gian không chính xác: close gần nhất trước
  event date.

T+1/T+3/T+5 là phiên giao dịch, không phải ngày lịch. Target thiếu không được
forward-fill.

### 4.2 Market features

| Feature | Ý nghĩa |
|---|---|
| `market_return_1d` | Return một phiên gần nhất |
| `market_intraday_range` | Biên độ high–low tại anchor |
| `market_open_gap` | Open so với previous close |
| `market_log1p_volume` | Log-transform volume |
| `market_volume_ratio_20` | Volume so với trung bình 20 phiên |
| `market_momentum_5` | Momentum năm phiên quá khứ |
| `market_momentum_20` | Momentum 20 phiên quá khứ |
| `market_volatility_20` | Volatility rolling 20 phiên |

Mọi rolling feature lấy từ anchor hoặc trước anchor, không lấy target close
hay future return.

### 4.3 Raw-return target

```text
stock_return_tN = target_adjusted_close_tN / observable_anchor_close - 1
```

Abnormal return có trong pipeline nghiên cứu nhưng không dùng làm primary label.

### 4.4 Phân biệt label contract

Gold dataset có contract ba lớp với neutral bands: T0/T+1 ±1,0%, T+3 ±1,5%
và T+5 ±2,0%. Tuy nhiên **binary experiment trong báo cáo không dùng neutral
bands**. Nó tạo lại label trực tiếp từ dấu raw return:

```text
return < 0 -> decrease
return > 0 -> increase
return = 0 hoặc missing -> mask
```

Do đó không nên mô tả là “xóa neutral”; chính xác là “binary sign prediction”.

| Horizon | Eligible | Decrease | Increase |
|---|---:|---:|---:|
| T+1 | 4.539 | 2.282 | 2.257 |
| T+3 | 4.859 | 2.313 | 2.546 |
| T+5 | 4.891 | 2.299 | 2.592 |

---

## 5. Historical-news context

History selection yêu cầu cùng ticker, ngày bài cũ nhỏ hơn ngày bài hiện tại,
lookback tối đa 14 ngày, không lấy cùng ngày, cosine similarity 0,30–0,85 và
tối đa năm bài. Current/history embeddings đều đến từ all-content model.

| Thống kê | Giá trị |
|---|---:|
| Rows có history | 3.815/5.068 |
| Coverage | 75,28% |
| Mean history count | 2,76 |
| Median | 3 |
| Maximum | 5 |

Threshold được giữ từ thí nghiệm trước và chưa recalibrate cho embedding mới;
history result hiện mang tính exploratory.

---

## 6. Hierarchical PhoBERT

### 6.1 Kiến trúc

```text
Title ----------+
Summary --------+
Content chunk 1 +
...             +--> shared PhoBERT --> segment vectors
Content chunk N +                            |
                                               v
                                  Additive Attention Pooling
                                               |
                                      article vector 768D
                                  +------------+------------+
                                  |            |            |
                               T+1 head     T+3 head     T+5 head
```

Title, summary và mỗi content chunk là segment độc lập; không nối toàn bài rồi
truncate.

| Thành phần | Cấu hình |
|---|---|
| Base encoder | `phobert_financial_simcse_e5` |
| Maximum tokens/segment | 192 |
| Content overlap | 32 |
| Maximum chunks | Không giới hạn |
| Token pooling | Attention-mask mean |
| Document pooling | Trainable additive attention |
| Position encoding | Sinusoidal |
| Article embedding | 768D |
| Attention hidden size | 256 |
| Dropout | 0,2 |

| Statistic | Segments/article, gồm title và summary |
|---|---:|
| Minimum | 3 |
| Median | 6 |
| P95 | 14 |
| Maximum | 46 |

### 6.2 Thành phần được train

PhoBERT được dùng để precompute segment embeddings và **đóng băng** trong lần
training cuối. Backpropagation chỉ cập nhật additive attention, LayerNorm và
ba binary heads. Tên chính xác là “frozen financial PhoBERT with trainable
hierarchical attention”, không phải end-to-end PhoBERT fine-tuning.

### 6.3 Loss và hyperparameters

Mỗi head dùng `BCEWithLogitsLoss(pos_weight=negatives/positives)`. Loss batch là
trung bình loss của các head có target hợp lệ.

| Hyperparameter | Giá trị |
|---|---:|
| Requested/completed epochs | 20 / 5 |
| Batch size | 64 |
| Article encode batch | 8 |
| Segment micro-batch | 32 |
| Optimizer | AdamW |
| Learning rate | 0,001 |
| Weight decay | 0,0001 |
| Gradient clipping | 1,0 |
| Early-stopping patience | 4 |
| Selection criterion | Mean validation Macro-F1 của ba heads |
| Seed | 42 |
| Decision threshold | 0,5 |

### 6.4 Training history

| Epoch | Train loss | Mean validation Macro-F1 |
|---:|---:|---:|
| 1 | 0,6724 | **0,5049** |
| 2 | 0,6393 | 0,4774 |
| 3 | 0,6287 | 0,4947 |
| 4 | 0,6217 | 0,4978 |
| 5 | 0,6161 | 0,5033 |

Checkpoint tốt nhất là epoch 1. Train loss giảm nhưng validation không vượt
epoch 1, cho thấy overfitting sớm.

### 6.5 Attention audit

| Split | Title | Summary | Content |
|---|---:|---:|---:|
| Validation | 0,114% | 0,012% | 99,874% |
| Test | 0,100% | 0,010% | 99,889% |

Model sử dụng full content nhưng attention collapse gần như hoàn toàn về
content. Attention mass không đồng nghĩa với causal importance.

---

## 7. Spark downstream training

| Feature set | Fulltext | Market | History |
|---|---:|---:|---:|
| Fulltext-only | Có | Không | Không |
| Fulltext + market | Có | Có | Không |
| Fulltext + market + history | Có | Có | Có |

Preprocessing được fit chỉ trên train: median imputation, StandardScaler,
VectorAssembler và một class-weighted Spark Logistic Regression cho mỗi
horizon.

| Tham số | Giá trị |
|---|---:|
| Family | Multinomial logistic regression |
| Maximum iterations | 300 |
| Regularization mặc định script | 0,05 |
| Elastic-net mặc định | 0,0 (L2) |
| Class weight | `N / (K * class_count)` |

`regParam` và `elasticNetParam` chưa được serialize vào metrics artifact; giá
trị trên là default của script. Lần sau cần lưu toàn bộ run config.

---

## 8. Experimental protocol

### 8.1 Fixed temporal split

```text
Train:      <= 31/12/2024       3.090 rows
Validation: năm 2025            1.389 rows
Test:       > 31/12/2025          589 rows
```

Split này kiểm tra tổng quát hóa từ dữ liệu cũ sang giai đoạn mới.

### 8.2 Expanded temporal split

```text
Train:      <= 31/12/2025       4.479 rows
Validation: 01/01–30/04/2026      242 rows
Test:       > 30/04/2026          347 rows
```

Split này mô phỏng retraining bằng data mới. Đây chưa phải rolling/walk-forward
evaluation đầy đủ vì chỉ có một mốc train mở rộng.

### 8.3 Leakage controls

- Temporal split, không random split.
- Imputer, scaler và classifier chỉ fit trên train.
- Target dates luôn sau event date và anchor.
- Historical retrieval chỉ lấy bài ngày trước.
- Audit không thấy overlap train–test theo event ID, canonical URL, content
  hash, exact title hoặc exact embedding.
- Một event luôn thuộc một split vì split theo event date.

Validation dùng để chọn attention checkpoint; test mới là final estimate trong
từng setting.

### 8.4 Metrics

Macro-F1 là metric chính. Metrics bổ sung: accuracy, weighted-F1, F1 từng lớp,
confusion matrix; hierarchical model còn có balanced accuracy, ROC-AUC, PR-AUC
và Brier score.

---

## 9. Kết quả Hierarchical PhoBERT multi-task

| Horizon | Rows | Accuracy | Balanced acc. | Macro-F1 | Weighted-F1 | ROC-AUC | PR-AUC | Brier | Majority acc. |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| T+1 | 549 | 0,5446 | 0,5468 | **0,5415** | 0,5480 | 0,5478 | 0,4771 | 0,2502 | 0,5865 |
| T+3 | 578 | 0,5571 | 0,5131 | 0,5034 | 0,5345 | 0,5259 | 0,4429 | 0,2459 | 0,5952 |
| T+5 | 576 | 0,4809 | 0,5129 | 0,4761 | 0,4674 | 0,5217 | 0,4298 | 0,2636 | 0,5868 |

Mean test Macro-F1 là 0,5070; không head nào vượt majority accuracy.

| Horizon | Class | Precision | Recall | F1 | Support |
|---|---|---:|---:|---:|---:|
| T+1 | Decrease | 0,6324 | 0,5342 | 0,5791 | 322 |
| T+1 | Increase | 0,4585 | 0,5595 | 0,5040 | 227 |
| T+3 | Decrease | 0,6038 | 0,7442 | 0,6667 | 344 |
| T+3 | Increase | 0,4286 | 0,2821 | 0,3402 | 234 |
| T+5 | Decrease | 0,6066 | 0,3284 | 0,4261 | 338 |
| T+5 | Increase | 0,4224 | 0,6975 | 0,5261 | 238 |

Confusion matrices:

| T+1 | T+3 | T+5 |
|---|---|---|
| `[[172,150],[100,127]]` | `[[256,88],[168,66]]` | `[[111,227],[72,166]]` |

---

## 10. Fixed temporal test

### 10.1 Macro-F1

| Feature set | T+1 | T+3 | T+5 | Mean |
|---|---:|---:|---:|---:|
| Fulltext-only | **0,4684** | 0,5208 | 0,5156 | 0,5016 |
| Fulltext + market | 0,4621 | **0,5318** | 0,4965 | 0,4968 |
| Fulltext + market + history | 0,4362 | 0,5037 | **0,5688** | **0,5029** |

### 10.2 Full test metrics

| Model | Horizon | Rows | Accuracy | Weighted-F1 | Macro-F1 | F1 down | F1 up | Confusion matrix |
|---|---|---:|---:|---:|---:|---:|---:|---|
| Fulltext-only | T+1 | 549 | 0,4991 | 0,4905 | 0,4684 | 0,5962 | 0,3405 | `[[203,119],[156,71]]` |
| Fulltext-only | T+3 | 578 | 0,5363 | 0,5372 | 0,5208 | 0,6070 | 0,4346 | `[[207,137],[131,103]]` |
| Fulltext-only | T+5 | 576 | 0,5156 | 0,5163 | 0,5156 | 0,5198 | 0,5114 | `[[151,187],[92,146]]` |
| Fulltext + market | T+1 | 549 | 0,4936 | 0,4846 | 0,4621 | 0,5924 | 0,3317 | `[[202,120],[158,69]]` |
| Fulltext + market | T+3 | 578 | 0,5484 | 0,5486 | 0,5318 | 0,6201 | 0,4435 | `[[213,131],[130,104]]` |
| Fulltext + market | T+5 | 576 | 0,4965 | 0,4971 | 0,4965 | 0,5000 | 0,4930 | `[[145,193],[97,141]]` |
| Fulltext + market + history | T+1 | 549 | 0,4645 | 0,4580 | 0,4362 | 0,5625 | 0,3099 | `[[189,133],[161,66]]` |
| Fulltext + market + history | T+3 | 578 | 0,5277 | 0,5245 | 0,5037 | 0,6128 | 0,3947 | `[[216,128],[145,89]]` |
| Fulltext + market + history | T+5 | 576 | **0,5747** | **0,5775** | **0,5688** | 0,6190 | 0,5187 | `[[199,139],[106,132]]` |

---

## 11. Expanded temporal test

### 11.1 Macro-F1

| Feature set | T+1 | T+3 | T+5 | Mean |
|---|---:|---:|---:|---:|
| Fulltext-only | 0,4353 | 0,5170 | 0,5377 | 0,4967 |
| Fulltext + market | **0,4574** | **0,5348** | 0,5467 | **0,5130** |
| Fulltext + market + history | 0,4377 | 0,5014 | **0,5976** | 0,5122 |

### 11.2 Full test metrics

| Model | Horizon | Rows | Accuracy | Weighted-F1 | Macro-F1 | F1 down | F1 up | Confusion matrix |
|---|---|---:|---:|---:|---:|---:|---:|---|
| Fulltext-only | T+1 | 329 | 0,4468 | 0,4424 | 0,4353 | 0,5160 | 0,3546 | `[[97,82],[100,50]]` |
| Fulltext-only | T+3 | 344 | 0,5174 | 0,5153 | 0,5170 | 0,5030 | 0,5311 | `[[84,109],[57,94]]` |
| Fulltext-only | T+5 | 341 | 0,5396 | 0,5355 | 0,5377 | 0,5078 | 0,5675 | `[[81,102],[55,103]]` |
| Fulltext + market | T+1 | 329 | 0,4650 | 0,4631 | 0,4574 | 0,5217 | 0,3931 | `[[96,83],[93,57]]` |
| Fulltext + market | T+3 | 344 | 0,5349 | 0,5342 | 0,5348 | 0,5294 | 0,5402 | `[[90,103],[57,94]]` |
| Fulltext + market | T+5 | 341 | 0,5484 | 0,5446 | 0,5467 | 0,5188 | 0,5746 | `[[83,100],[54,104]]` |
| Fulltext + market + history | T+1 | 329 | 0,4377 | 0,4374 | 0,4377 | 0,4343 | 0,4411 | `[[71,108],[77,73]]` |
| Fulltext + market + history | T+3 | 344 | 0,5029 | 0,5047 | 0,5014 | 0,5289 | 0,4738 | `[[96,97],[74,77]]` |
| Fulltext + market + history | T+5 | 341 | **0,5982** | **0,5964** | **0,5976** | 0,5810 | 0,6141 | `[[95,88],[49,109]]` |

### 11.3 Validation summary

Mỗi ô là `accuracy / Macro-F1`.

| Feature set | T+1 | T+3 | T+5 |
|---|---:|---:|---:|
| Fulltext-only | 0,6091 / 0,6070 | 0,4658 / 0,4656 | 0,4085 / 0,4083 |
| Fulltext + market | 0,5955 / 0,5924 | 0,4829 / 0,4821 | 0,4255 / 0,4240 |
| Fulltext + market + history | 0,6136 / 0,6094 | 0,5214 / 0,5144 | 0,5277 / 0,5255 |

Khoảng cách validation–test lớn ở một số horizon cho thấy distribution/regime
shift và test size nhỏ; không nên chọn model chỉ từ một ô kết quả cao.

---

## 12. Ablation và phân tích

### 12.1 Tổng hợp mean Macro-F1

| Feature set | Fixed test | Expanded test |
|---|---:|---:|
| Fulltext-only | 0,5016 | 0,4967 |
| Fulltext + market | 0,4968 | **0,5130** |
| Fulltext + market + history | **0,5029** | 0,5122 |

Market làm fixed mean giảm 0,0048 nhưng expanded mean tăng 0,0163. Feature
thị trường có tín hiệu nhưng phụ thuộc market regime.

### 12.2 History tại T+5

| Split | Không history | Có history | Delta Macro-F1 |
|---|---:|---:|---:|
| Fixed | 0,4965 | 0,5688 | +0,0723 |
| Expanded | 0,5467 | 0,5976 | +0,0509 |

History giúp T+5 ở cả hai settings nhưng làm T+1/T+3 giảm trong expanded test.
Chuỗi tin cũ có thể phù hợp với phản ứng nhiều phiên nhưng gây nhiễu ngắn hạn.

### 12.3 Majority baseline

| Horizon | Expanded majority accuracy | Best model accuracy |
|---|---:|---:|
| T+1 | 0,5441 | 0,4650 |
| T+3 | 0,5610 | 0,5349 |
| T+5 | 0,5367 | **0,5982** |

Chỉ T+5 tốt nhất vượt majority accuracy. Macro-F1 vẫn cần thiết vì majority
classifier bỏ lớp thiểu số, nhưng T+1/T+3 chưa tạo predictive edge ổn định.

### 12.4 Vì sao T+1 yếu

- Daily data không tách chuyển động trước/sau publication trong cùng phiên.
- Phản ứng ngắn hạn chịu ảnh hưởng thị trường chung và tin đồng thời.
- Historical publication time đôi khi chỉ chính xác theo ngày.
- Thông tin có thể đã được kỳ vọng trước khi bài đăng.
- Binary sign label rất nhạy với return nhỏ gần 0.

### 12.5 Full article và attention

Model thực sự dùng content, nhưng full-content không cải thiện đồng đều. Nội
dung dài chứa boilerplate hoặc phần ít liên quan; publisher cũng có thể cập
nhật bài sau publication. Gần 100% attention mass ở content không chứng minh
quan hệ nhân quả với return.

### 12.6 Regime/source shift

Historical data chủ yếu đến qua Google News aggregation, dữ liệu mới đến từ
năm RSS trực tiếp. Khác biệt nguồn, phong cách và giai đoạn thị trường tạo
distribution shift. Việc thứ hạng model đổi giữa fixed/expanded là bằng chứng
rằng một temporal holdout chưa đủ.

### 12.7 Ý nghĩa thực tế

T+5 Macro-F1 0,5976 là tín hiệu nghiên cứu, không phải bằng chứng lợi nhuận.
Report chưa xét transaction cost, slippage, portfolio construction, turnover,
risk-adjusted return và statistical significance qua nhiều regimes.

### 12.8 Permutation feature importance

Importance được tính bằng cách **tráo giá trị của một feature giữa các mẫu ở
validation/test**, sau đó chạy lại dự đoán bằng model đã train và đo Macro-F1.
Ví dụ, một feature có giá trị `[0,01; -0,02; 0,005]` có thể thành
`[0,005; 0,01; -0,02]`. Cột vẫn còn và phân phối giá trị vẫn tương tự, nhưng
giá trị không còn gắn với đúng bài báo/ticker ban đầu.

Đây không phải xóa cột rồi train lại. Model và các trọng số được giữ nguyên;
chỉ dữ liệu của feature bị tráo trong bước đánh giá. Mỗi feature được tráo 30
lần để giảm phụ thuộc vào một hoán vị ngẫu nhiên. Điểm được định nghĩa:

```text
importance = baseline Macro-F1 - mean Macro-F1 sau khi tráo feature
```

Điểm dương lớn nghĩa là model hiện tại cần thông tin đó để giữ hiệu quả trên
split đang xét. Điểm gần 0 nghĩa là dự đoán ít đổi. Điểm âm nghĩa là khi tráo
feature, điểm lại tăng; feature có thể gây nhiễu hoặc model có thể overfit vào
nó trong split này. Điểm này đo phụ thuộc của model, không chứng minh quan hệ
nhân quả.

PhoBERT article/history là vector 768 chiều nên mỗi vector được tráo nguyên
khối và có một điểm importance cho cả nhóm. Mỗi market/history scalar được
tráo riêng, cho một điểm riêng. Các tọa độ PhoBERT không được diễn giải riêng
vì chúng không tương ứng trực tiếp với biến có tên dễ hiểu.

Ví dụ trên expanded test của `Fulltext + market + history`, tại T+5:

| Feature | Macro-F1 giảm sau permutation |
|---|---:|
| History embedding (768D, tráo như một nhóm) | 0,0784 |
| Current article embedding (768D, tráo như một nhóm) | 0,0632 |
| Market momentum 5 phiên | 0,0147 |
| History availability | 0,0135 |
| Market momentum 20 phiên | 0,0130 |
| Market return 1 phiên | 0,0116 |

Ví dụ `0,0784` nghĩa là Macro-F1 gốc 0,5976 giảm trung bình khoảng 0,0784
khi history embedding bị tráo; đây không phải tỷ lệ phần trăm accuracy hay
mức tác động nhân quả. Ở expanded T+1 của model `Fulltext + market`,
`market_return_1d` có importance 0,0237, cao nhất trong các feature của model
đó. Importance thay đổi theo horizon và temporal split; expanded T+1 khi có
history thậm chí cho điểm âm với text embeddings. Chi tiết validation/test,
độ lệch qua 30 lần và mọi feature nằm trong
`results/fulltext_model/FEATURE_IMPORTANCE_REPORT.md` và
`results/fulltext_model/feature_importance_permutation.json`.

Permutation làm vỡ tương quan tự nhiên giữa các feature; các feature tương quan
có thể chia sẻ hoặc che lấp importance. Những điểm nhỏ hơn độ lệch giữa các
lần tráo chưa phải bằng chứng đóng góp ổn định. Cần xác nhận trên nhiều
walk-forward folds và bootstrap theo event/ticker-date.

---

## 13. Hạn chế và rủi ro

1. Expanded test chỉ có 329–344 mẫu/horizon.
2. Rows cùng article hoặc ticker-date không độc lập.
3. Chưa có walk-forward nhiều folds.
4. Chưa có cluster bootstrap confidence interval cho model hiện tại.
5. History threshold chưa tune cho embedding mới.
6. Attention collapse về content.
7. Publisher có thể cập nhật article sau publication.
8. Historical publication time có thể chỉ là proxy.
9. Binary sign label biến return rất nhỏ thành tăng/giảm.
10. Ticker extraction error có thể gắn sai doanh nghiệp.
11. Ảnh chưa tham gia model.
12. Hyperparameter search/negative controls chưa hoàn chỉnh.
13. Metrics artifact chưa lưu toàn bộ run config.
14. Chưa đánh giá khả năng giao dịch thực tế.

---

## 14. Kết luận

Pipeline đã hoàn thành chuỗi từ news ingestion, full-article extraction, ticker
linking, point-in-time market join, Gold targets đến temporal evaluation. Dataset
cuối phủ đủ 30 mã VN30, 2.948 articles và 5.068 event–ticker pairs.

1. `Fulltext + market` tốt nhất theo expanded mean Macro-F1: **0,5130**.
2. `Fulltext + market + history` tốt nhất tại T+5: accuracy **0,5982** và
   Macro-F1 **0,5976**.
3. T+1/T+3 chưa vượt majority accuracy và kết quả thay đổi theo temporal split.

> Nội dung bài báo, trạng thái thị trường và chuỗi tin lịch sử chứa một phần tín
> hiệu liên quan đến hướng raw return, rõ nhất tại T+5. Tuy nhiên tín hiệu chưa
> ổn định qua horizon và market regime. Kết quả xác nhận tính khả thi của
> pipeline nghiên cứu, chưa xác nhận khả năng tạo lợi nhuận hoặc sẵn sàng giao
> dịch.

---

## 15. Hướng phát triển

1. Walk-forward evaluation nhiều folds; báo cáo mean ± standard deviation.
2. Bootstrap theo unique event và ticker-date cluster.
3. Label-permutation negative control.
4. Giữ final holdout chưa dùng để chọn model/threshold.
5. So sánh binary sign với three-class neutral-threshold prediction.
6. Recalibrate history similarity trong validation từng fold.
7. Two-stage attention: pool content rồi fusion title/summary.
8. Field dropout để giảm attention collapse.
9. Lưu article snapshots theo `collected_at` và versioned content hash.
10. Chỉ thêm image branch sau khi image quality filtering hoàn chỉnh.
11. Bổ sung calibration, transaction cost và backtest sau khi predictive
    evaluation vượt baseline ổn định.

---

## 16. Reproducibility và artifacts

| Artifact | Đường dẫn local |
|---|---|
| Full-content Gold | `data_lake/gold/training_targets_fulltext_attention_v1` |
| Segment cache | `data/hierarchical_phobert_all_content_cache_v2.pt` |
| Hierarchical model | `models/hierarchical_phobert_all_content_binary_v2` |
| Fixed fulltext-only | `models/ablation_binary_fulltext_text_only_v1` |
| Fixed fulltext + market | `models/ablation_binary_fulltext_text_market_v1` |
| Fixed fulltext + market + history | `models/ablation_binary_fulltext_text_market_history_v1` |
| Expanded fulltext-only | `models/ablation_binary_fulltext_rolling_v2_text_only` |
| Expanded fulltext + market | `models/ablation_binary_fulltext_rolling_v2_text_market` |
| Expanded fulltext + market + history | `models/ablation_binary_fulltext_rolling_v2_text_market_history` |
| Hierarchical training | `news_system/train_hierarchical_phobert.py` |
| Spark ablation training | `news_system/spark_jobs/train_movement_baseline.py` |
| History builder | `news_system/build_fulltext_history_embeddings.py` |
| Permutation importance report | `results/fulltext_model/FEATURE_IMPORTANCE_REPORT.md` |
| Permutation importance detail | `results/fulltext_model/feature_importance_permutation.json` |
| Permutation importance script | `news_system/spark_jobs/feature_importance_permutation.py` |

Dataset, raw article, ảnh, database và cache không nên public nếu chưa kiểm tra
bản quyền, quyền riêng tư và điều khoản nguồn. Model release chỉ nên chứa
weights, inference code, sanitized metrics, model card và report này.
