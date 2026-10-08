# EDA Reports

EDA có cả notebook tổng hợp để trình bày và ba notebook riêng để phân tích từng
dataset. Notebook tổng hợp vẫn chạy tuần tự và mỗi cell chỉ hiển thị một bảng.

| Dataset | Notebook | Báo cáo |
|---|---|---|
| Tổng hợp | `eda/COMBINED_EDA.ipynb` | `eda/COMBINED_EDA_REPORT.md` |
| Silver News | `eda/NEWS_EDA.ipynb` | `eda/NEWS_EDA_REPORT.md` |
| Silver Market | `eda/MARKET_EDA.ipynb` | `eda/MARKET_EDA_REPORT.md` |
| Gold Event–Market | `eda/IMPACT_EDA.ipynb` | `eda/IMPACT_EDA_REPORT.md` |

Các notebook output/HTML được tạo local nhưng không commit. Chạy toàn bộ bằng:

```bash
jupyter nbconvert --to notebook --execute eda/COMBINED_EDA.ipynb \
  --output COMBINED_EDA.executed.ipynb --output-dir eda
```

Hoặc chạy lại riêng từng phần bằng:

```bash
jupyter nbconvert --to notebook --execute eda/NEWS_EDA.ipynb \
  --output NEWS_EDA.executed.ipynb

jupyter nbconvert --to notebook --execute eda/MARKET_EDA.ipynb \
  --output MARKET_EDA.executed.ipynb

jupyter nbconvert --to notebook --execute eda/IMPACT_EDA.ipynb \
  --output IMPACT_EDA.executed.ipynb
```
