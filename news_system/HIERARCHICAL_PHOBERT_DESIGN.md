# Hierarchical PhoBERT text branch

## Input contract

Nhánh text nhận ba trường riêng biệt:

- `title`;
- `summary`;
- `content` đã làm sạch và có ít nhất 200 ký tự.

Không nối toàn bộ ba trường rồi truncate. Title, summary và từng content chunk là các segment độc lập.

## Flow

```text
title --------------------> PhoBERT -- mean token pooling --+
summary ------------------> PhoBERT -- mean token pooling --+
content chunk 1 ----------> PhoBERT -- mean token pooling --+
content chunk 2 ----------> PhoBERT -- mean token pooling --+--> Attention Pooling
...                                                        |          |
content chunk N ----------> PhoBERT -- mean token pooling --+          v
                                                               article embedding 768
```

- PhoBERT được chia sẻ cho mọi segment.
- Content được chia đến hết bài, không giới hạn số chunk.
- Mỗi segment tối đa 192 tokens, overlap content 32 tokens.
- Token vector được gộp bằng attention-mask mean pooling.
- Segment vector được bổ sung field embedding và sinusoidal position encoding trước khi tính attention score; position encoding không đặt giới hạn độ dài bài.
- Weighted sum dùng original segment vectors để tạo article embedding.
- Model trả cả `attention_weights` để audit title/summary/chunk nào ảnh hưởng prediction.

## Attention

Với segment vector `h_i`, field embedding `f_i` và position embedding `p_i`:

```text
e_i = vᵀ tanh(W(h_i + f_i + p_i))
alpha_i = masked_softmax(e_i)
article = Σ alpha_i h_i
```

Padding segment luôn có weight bằng 0. Tổng attention của các segment hợp lệ bằng 1.

## Training stages

1. Freeze PhoBERT, chỉ train attention pooler và classification heads.
2. So sánh với title + summary baseline trên cùng walk-forward folds.
3. Nếu cải thiện ổn định, unfreeze tối đa 1–2 layer cuối với learning rate nhỏ hơn head.
4. Chỉ sau đó mới nối market/history/image branches.

## Ablation bắt buộc

| ID | Text input | Pooling |
|---|---|---|
| T0 | title + summary | mean pooling hiện tại |
| T1 | title + summary + first content chunk | attention |
| T2 | title + summary + all bounded chunks | mean segment pooling |
| T3 | title + summary + all bounded chunks | attention pooling |

T3 chỉ được giữ nếu thắng T0/T2 trên nhiều temporal folds, không chỉ một test period.

## Leakage requirements

- Content phải là snapshot có tại prediction time.
- Không dùng bài được cập nhật sau market outcome nếu không có snapshot gốc.
- Chunking và PhoBERT không đọc target columns.
- Attention pooler chỉ fit trên train.
- Validation/test giữ nguyên thứ tự thời gian và dedup theo event/content.

