# Kafka local + Spark Streaming + Azure Data Lake

Luồng chính:

```text
RSS producer -> Kafka local -> Spark Structured Streaming -> ADLS Bronze
                                                    |
                                                    v
                                           Spark batch Silver/Gold
```

Batch collector cũ vẫn giữ để backfill và làm fallback khi máy local tắt.

## 1. Yêu cầu local

- Docker Engine và Docker Compose plugin
- Java 17
- Conda environment `tf_linux`
- Azure CLI đã `az login`
- Quyền `Storage Blob Data Contributor` trên Storage Account của dự án

Kiểm tra:

```bash
docker --version
docker compose version
java -version
az account show --query name -o tsv
```

## 2. Khởi động Kafka

```bash
docker compose -f news_system/deploy/kafka/compose.yaml up -d
docker compose -f news_system/deploy/kafka/compose.yaml ps
```

Kafka UI:

```text
http://localhost:8080
```

Ba topic được tạo tự động: `news.raw.v1`, `market.raw.v1` và
`pipeline.dead-letter.v1`.

Không dùng `docker compose down -v` trừ khi muốn xóa toàn bộ Kafka log.

## 3. Cài Python dependency

```bash
conda activate tf_linux
python -m pip install \
  -r news_system/requirements-kafka.txt \
  -r news_system/requirements-azure.txt \
  -r news_system/requirements-spark.txt
```

## 4. Gửi news vào Kafka

Lượt test hai full article:

```bash
python -m news_system.kafka_producer \
  --bootstrap-servers localhost:9092 \
  --topic news.raw.v1 \
  --state-db data/kafka_producer_state_test.db \
  --article-limit 2
```

Production liên tục:

```bash
python -m news_system.kafka_producer \
  --bootstrap-servers localhost:9092 \
  --topic news.raw.v1 \
  --state-db data/kafka_producer_state.db \
  --article-limit 0 \
  --watch --interval-minutes 15
```

Producer bật idempotence, `acks=all`, gzip compression và chỉ đánh dấu local
state sau khi broker xác nhận delivery.

## 5. Spark Kafka -> ADLS Bronze

Spark Kafka connector phải khớp Spark 3.5.7/Scala 2.12:

```bash
PYSPARK_PYTHON="$(which python)" PYSPARK_DRIVER_PYTHON="$(which python)" spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 \
  news_system/spark_jobs/kafka_to_bronze.py \
  --bootstrap-servers localhost:9092 \
  --topic news.raw.v1 \
  --account-name <storage-account> \
  --checkpoint data_lake/checkpoints/news_kafka_to_bronze \
  --trigger continuous \
  --interval '30 seconds'
```

Spark dùng Azure CLI credential qua `DefaultAzureCredential`. Output nằm tại:

```text
bronze/news_stream/ingest_date=YYYY-MM-DD/hour=HH/spark_batch_*.jsonl.gz
```

Mỗi record chứa raw Kafka payload cùng `topic`, `partition`, `offset` và Kafka
timestamp. Checkpoint local bảo đảm Spark tiếp tục từ offset đã xử lý.

Để xử lý backlog rồi dừng:

```bash
PYSPARK_PYTHON="$(which python)" PYSPARK_DRIVER_PYTHON="$(which python)" spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 \
  news_system/spark_jobs/kafka_to_bronze.py \
  --bootstrap-servers localhost:9092 \
  --topic news.raw.v1 \
  --account-name <storage-account> \
  --checkpoint data_lake/checkpoints/news_kafka_to_bronze \
  --trigger available-now
```

## 6. Vai trò Azure VM

VM 1 GB không chạy Kafka hoặc Spark. Nó chỉ chạy `batch_collector` để thu thập
thẳng vào `bronze/news` khi local tắt. Hai luồng có prefix riêng:

```text
bronze/news/         batch fallback từ Azure VM
bronze/news_stream/  Kafka + Spark từ local
```

Silver job sau này đọc cả hai prefix và deduplicate bằng `event_id`.
