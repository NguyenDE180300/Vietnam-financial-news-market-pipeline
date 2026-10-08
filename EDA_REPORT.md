# EDA Reports

EDA đã được tách hoàn toàn theo từng dataset; file này chỉ là mục lục, không gộp
kết quả News, Market và Gold vào cùng một báo cáo.

| Dataset | Notebook | Báo cáo |
|---|---|---|
| Silver News | `eda/NEWS_EDA.ipynb` | `eda/NEWS_EDA_REPORT.md` |
| Silver Market | `eda/MARKET_EDA.ipynb` | `eda/MARKET_EDA_REPORT.md` |
| Gold Event–Market | `eda/IMPACT_EDA.ipynb` | `eda/IMPACT_EDA_REPORT.md` |

Các notebook output/HTML được tạo local nhưng không commit. Chạy lại riêng từng
phần bằng:

```bash
jupyter nbconvert --to notebook --execute eda/NEWS_EDA.ipynb \
  --output NEWS_EDA.executed.ipynb

jupyter nbconvert --to notebook --execute eda/MARKET_EDA.ipynb \
  --output MARKET_EDA.executed.ipynb

jupyter nbconvert --to notebook --execute eda/IMPACT_EDA.ipynb \
  --output IMPACT_EDA.executed.ipynb
```
