# Hybrid batch setup: Azure Student + local Spark

Kiến trúc triển khai:

```text
Azure VM 1 GB                       Máy local
RSS/full-article collector          Spark batch
          |                              ^  |
          v                              |  v
       ADLS Bronze ----------------------+  ADLS Silver/Gold
```

Kafka không nằm trong pipeline này. Collector chạy mỗi 15 phút; Spark chạy theo
lịch hoặc thủ công và không cần hoạt động liên tục.

## 1. Tạo Data Lake trên Azure

Trong Azure Portal, tạo Storage Account với:

- `StorageV2`
- `Standard`
- `LRS`
- `Hierarchical namespace: Enabled`

Tạo các container/file system:

```text
bronze
silver
gold
checkpoints
```

Không lưu account key hoặc SAS token trong repository.

## 2. Cho Azure VM quyền ghi ADLS

Trong trang VM, bật `System assigned managed identity`. Tại Storage Account,
gán identity của VM role `Storage Blob Data Contributor`.

Collector dùng `DefaultAzureCredential`, vì vậy trên VM nó tự dùng managed
identity và không cần secret. Kiểm tra role đã có hiệu lực trước khi chạy job.

## 3. Chuẩn bị Azure VM

Đưa repository vào `/opt/dsp301m` và dùng Python 3.10+ trong environment đang
chạy ứng dụng. File service mẫu giả định interpreter ở:

```text
/opt/miniconda3/envs/tf_linux/bin/python
```

Nếu đường dẫn thực tế khác, sửa `ExecStart` trong file service.

Cài dependency bằng chính interpreter đó:

```bash
cd /opt/dsp301m
/opt/miniconda3/envs/tf_linux/bin/python -m pip install \
  -r news_system/requirements-ui.txt \
  -r news_system/requirements-azure.txt
```

Tạo thư mục runtime:

```bash
sudo mkdir -p /etc/vn-news /var/lib/vn-news
sudo chown -R azureuser:azureuser /var/lib/vn-news
sudo chmod 750 /etc/vn-news
```

Tạo `/etc/vn-news/collector.env` trên VM:

```text
AZURE_STORAGE_ACCOUNT=ten_storage_account
```

File này chỉ chứa tên account, không chứa credential. Kiểm tra một lượt thủ
công trước:

```bash
cd /opt/dsp301m
/opt/miniconda3/envs/tf_linux/bin/python -m news_system.batch_collector \
  --backend adls \
  --state-db /var/lib/vn-news/collector_state.db \
  --article-limit 2
```

Sau khi thấy file trong `bronze/news/...`, cài timer:

```bash
sudo cp news_system/deploy/vn-news-collector.service /etc/systemd/system/
sudo cp news_system/deploy/vn-news-collector.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now vn-news-collector.timer
systemctl list-timers vn-news-collector.timer
```

Theo dõi:

```bash
journalctl -u vn-news-collector.service -f
```

Collector chỉ ghi các bài chưa upload. Trạng thái nằm trong
`/var/lib/vn-news/collector_state.db`; trạng thái chỉ được đánh dấu sau khi ADLS
upload thành công.

## 4. Kiểm thử Data Lake local trước khi dùng Azure

Không cần Azure credential:

```bash
python -m news_system.batch_collector \
  --backend local \
  --root data_lake \
  --state-db data_lake/collector_state.db \
  --article-limit 2
```

Kết quả:

```text
data_lake/bronze/news/ingest_date=YYYY-MM-DD/hour=HH/batch_*.jsonl.gz
```

Chạy lại cùng state DB sẽ không phát lại các URL đã ghi thành công.

## 5. Chuẩn bị Spark trên máy local

Spark yêu cầu Java. Kiểm tra:

```bash
java -version
python --version
```

Cài PySpark vào environment dành cho Spark:

```bash
python -m pip install -r news_system/requirements-spark.txt
```

Trước tiên kiểm tra bằng Bronze local:

```bash
PYSPARK_PYTHON="$(which python)" PYSPARK_DRIVER_PYTHON="$(which python)" \
spark-submit news_system/spark_jobs/bronze_to_silver.py \
  --input 'data_lake/bronze/news' \
  --output 'data_lake/silver/news' \
  --mode overwrite
```

Job thực hiện schema validation cơ bản, chuẩn hóa timestamp UTC, tạo
`content_hash`, loại trùng `event_id` và ghi Parquet partition theo
`published_date`.

## 6. Đồng bộ ADLS về local

Cách ít lỗi connector nhất cho MVP là dùng AzCopy thay vì cho Spark truy cập
`abfss://` trực tiếp:

```bash
azcopy login
azcopy copy \
  'https://ten_storage_account.dfs.core.windows.net/bronze/news' \
  'data_lake/bronze/news' \
  --recursive
```

Chạy Spark như bước 5, sau đó upload Silver:

```bash
azcopy copy \
  'data_lake/silver/news' \
  'https://ten_storage_account.dfs.core.windows.net/silver/news' \
  --recursive
```

Tài khoản đăng nhập local cần role `Storage Blob Data Contributor`. Không mở
public write access cho container.

## 7. Lịch vận hành

```text
Mỗi 15 phút  Azure timer     RSS/full article -> Bronze
Mỗi 1 giờ    Local/manual    Bronze -> Silver
Sau 16:00    Market batch    Giá daily -> Silver market
Sau 17:00    Local Spark     News-market join -> Gold
```

Nếu máy local không bật, Bronze vẫn tiếp tục được Azure VM thu thập. Lần sau
AzCopy tải các file về và Spark xử lý lại toàn bộ Bronze; `event_id` bảo đảm
Silver không có bài trùng trong lần build đó.

## 8. Bước tiếp theo

Pipeline hiện mới có `news Bronze -> news Silver`. Các bước tiếp theo là:

1. Market collector ghi `bronze/market`.
2. Spark tạo `silver/market_daily`.
3. Chạy rule-based + CRF NER và ghi `silver/news_tickers`.
4. Spark point-in-time join thành `gold/news_market_impact`.
5. Streamlit đọc Gold hoặc một SQLite serving cache được tạo từ Gold.

## 9. Market Bronze -> Silver (đã triển khai)

Thu VN30 daily bars từ Yahoo về local Bronze. Daily bar dùng
`session_date` làm khóa thời gian; `collected_at_utc` vẫn được lưu theo UTC.

```bash
python -m news_system.market_batch_collector \
  --backend local --root data_lake \
  --provider yahoo --vn30 --period 1mo
```

Chuẩn hóa, dedup và tính `daily_return`/`log_return` bằng Spark:

```bash
spark-submit news_system/spark_jobs/market_bronze_to_silver.py \
  --input data_lake/bronze/market \
  --output data_lake/silver/market_daily \
  --mode overwrite --output-partitions 4
```

Chỉ upload sau khi quality gate pass:

```bash
spark-submit news_system/spark_jobs/validate_market_silver.py \
  --input data_lake/silver/market_daily \
  --expected-min-tickers 30
```

ADLS layout:

```text
bronze/market/ingest_date=YYYY-MM-DD/batch_*.jsonl.gz
silver/market_daily/session_date=YYYY-MM-DD/*.parquet
```

Job Silver kiểm tra giá dương, quan hệ OHLC, volume không âm, phiên cuối
tuần, trùng khóa `(ticker, session_date, interval, source)` và biến động tuyệt
đối trên 50%. Yahoo được gọi với `auto_adjust=True`, vì vậy OHLC dùng cho event
study đã điều chỉnh split/cổ tức và trường `price_adjustment` ghi rõ nguồn gốc.

## 10. Chạy toàn bộ daily pipeline

Sau khi Kafka/Spark đã ghi News Bronze về local, chạy một lệnh:

```bash
python -m news_system.run_daily_pipeline
```

Lệnh dừng ngay nếu bất kỳ quality gate nào lỗi. Xem trước các lệnh mà không
chạy bằng `--dry-run`. Hai file systemd mẫu trong `news_system/deploy` đặt lịch
17:15 từ thứ Hai đến thứ Sáu; chúng không tự được cài hoặc bật.

Mở dashboard Gold:

```bash
streamlit run news_system/streamlit_app.py
```
