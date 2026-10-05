# Lakehouse — phiên bản Docker Compose

Bản chuyển đổi của stack K3s/Helm (`../helm/lakehouse`) sang **Docker Compose**, nhẹ hơn cho máy cá nhân: không có control-plane K3s, không cần `k3d image import`, code được bind-mount nên sửa là chạy.

```text
PostgreSQL (db orders) → Debezium → Kafka (KRaft)
  → Spark 4.0.4 + Delta 4.0.1 trên MinIO (bronze → silver → gold), Unity Catalog OSS 0.6 (catalog `lakehouse`)
  → Trino (catalog delta, file metastore trên MinIO) → Metabase / Grafana / FastAPI chatbot
```

Thư mục này **độc lập**: chứa bản copy của `spark/`, `airflow/`, `api/`, `trino/`, `grafana/`, `connectors/`, `Script/`. Chỉ có dataset CSV là dùng chung (mặc định `../dataset`, đổi bằng `DATASET_DIR` trong `.env`).

## Yêu cầu

- Docker Desktop (WSL2 backend), cấp **≥ 8 GB RAM** cho WSL2 (core stack ~7–8 GB khi chạy ETL; thêm profile `apps` cần ~10–11 GB).
- PowerShell (Windows).

## Khởi chạy

```powershell
cd lakehouse_docker
Copy-Item .env.example .env          # điền MIMO_API_KEY nếu dùng chatbot

docker compose build                 # build spark-lakehouse, airflow-lakehouse, fastapi
docker compose up -d                 # core ETL stack
docker compose ps                    # chờ các service healthy; minio-init & connect-init "Exited (0)"

.\scripts\seed-data.ps1              # nạp CSV Olist + Clickstream vào Postgres → Debezium đẩy CDC vào Kafka
```

Sau đó mở Airflow (http://localhost:8084, `airflow`/`airflow`) và trigger DAG **`lakehouse_etl_pipeline`**.

Bật thêm BI/UI (thay cho `values-light.yaml` / `toggle-services.ps1`):

```powershell
docker compose --profile apps up -d      # Metabase, Grafana, Kafka UI, FastAPI
docker compose --profile apps stop metabase grafana kafka-ui fastapi   # tắt bớt khi chạy ETL nặng
```

## Dịch vụ & cổng

| Dịch vụ | URL | Đăng nhập |
|---|---|---|
| Airflow | http://localhost:8084 | `airflow` / `airflow` |
| Spark Master UI | http://localhost:8080 | — |
| Trino | http://localhost:8081 | user `admin` |
| Unity Catalog API | http://localhost:8086/api/2.1/unity-catalog/catalogs | — |
| Kafka Connect API | http://localhost:8083/connectors | — |
| MinIO Console | http://localhost:9001 (S3 API :9000) | `admin` / `password123` |
| PostgreSQL | localhost:5432 | `postgres` / `123456` |
| Kafka (từ host) | localhost:29092 | — |
| Metabase *(apps)* | http://localhost:3000 | tạo lần đầu |
| Grafana *(apps)* | http://localhost:3001 | `admin` / `admin` |
| Kafka UI *(apps)* | http://localhost:8085 | — |
| FastAPI / Chatbot *(apps)* | http://localhost:8000 | — |

Cổng host giữ giống `scripts/port-forward-all.ps1` của bản K3s.

## Sửa code

| Thay đổi | Cần làm |
|---|---|
| `spark/app/**`, `airflow/dags/**` | Không cần gì — bind-mount, task Airflow kế tiếp dùng code mới |
| `api/**` (.py, .sql, static) | Không cần gì — uvicorn `--reload` |
| `requirements.txt`, `Dockerfile` | `docker compose build <service>` rồi `docker compose up -d <service>` |
| `docker-compose.yml`, `.env`, file config mount | `docker compose up -d` (tự recreate service bị đổi) |

## Lệnh thường dùng

```powershell
docker compose logs -f airflow                      # xem log
docker compose exec postgres psql -U postgres -d orders
.\scripts\register-connectors.ps1                   # đăng ký lại Debezium connectors
docker compose down                                 # dừng, GIỮ dữ liệu (volumes)
docker compose down -v                              # dừng và XÓA toàn bộ dữ liệu
```

## Khác biệt so với bản K3s

| K3s / Helm | Docker Compose |
|---|---|
| `values.yaml` → `secrets.*` | `.env` (gitignored), mẫu `.env.example` |
| Job `minio-init-buckets`, `kafka-connectors-init` (Helm hook) | Service one-shot `minio-init`, `connect-init` |
| `values-light.yaml`, `toggle-services.ps1` | Compose profile `apps` + `mem_limit` trong `.env` |
| `port-forward-all.ps1`, Ingress | `ports:` publish trực tiếp |
| Build → `k3d image import` → `rollout restart` | Bind-mount, không cần rebuild |
| ArgoCD / GitOps | Không dùng |

Lưu ý: `trino/catalog/delta.properties`, `connectors/*.json` vẫn hard-code `postgres/123456` và `admin/password123` như bản gốc — nếu đổi mật khẩu trong `.env` phải sửa cả các file này.

## Catalog: Unity Catalog + Trino

Hive Metastore đã được thay bằng **Unity Catalog OSS** (`unitycatalog`, H2 trong volume `uc-data`).

- Spark ghi Delta theo path (`s3a://<layer>/<table>/`), sau đó task `register_*_tables` (`jobs/<layer>/register_tables.py` → `common.catalog`) đăng ký mọi bảng trong `common/tables.py` dưới dạng **external** vào 2 nơi:
  - UC: `lakehouse.<bronze|silver|gold>.<table>` với `LOCATION 's3://…'` (UC chỉ nhận scheme `s3://`).
  - Trino: `CALL delta.system.register_table(...)` vào file metastore `s3://trino-metastore/` — vì Trino chưa đọc được Delta từ UC OSS. Tên `delta.gold.*` cho API/Grafana/Metabase giữ nguyên.
- UC vend credential tạm qua **STS AssumeRole của MinIO** (`AWS_ENDPOINT_URL_STS`) bằng user `UC_S3_ACCESS_KEY` do `minio-init` tạo (MinIO không cho root user AssumeRole).
- Query thử từ Spark: `spark.sql("SELECT * FROM lakehouse.gold.dim_date LIMIT 5")`.

## Grafana

`--profile apps` → http://localhost:3001 (`admin` / `admin`). Mọi thứ được provision tự động từ `grafana/`:

- Plugin `trino-datasource` (pin 1.2.0, cài khi container khởi động — cần internet lần đầu).
- Datasource `Trino (Delta Lake)` (uid `trino-delta`) → `http://trino:8080`, user `admin`.
- Dashboard **Lakehouse Overview** (folder *Lakehouse*): Olist (doanh thu theo tháng/bang, top category, review) và Clickstream (funnel, device, country, session). Query lấy từ `api/sql/`, đọc `delta.gold.*` nên cần chạy xong pipeline Airflow trước.
- Sửa `grafana/dashboards/*.json` → Grafana tự nạp lại sau ~30 giây (không cần restart).

## Data quality

Mỗi layer có một task gate `dq_bronze` / `dq_silver` / `dq_gold` (chạy `spark/app/jobs/ops/data_quality.py --layer <layer>`) ngay sau bước đăng ký bảng. Gate fail (exit 1) thì layer kế tiếp **không chạy**.

| Layer | Kiểm tra |
|---|---|
| Bronze | bảng đọc được & không rỗng; envelope CDC hợp lệ (`op` ∈ c/r/u, `ts_ms` > 0, có `after`); business key không null (bỏ qua event `d`). Không check trùng key vì bronze là log CDC |
| Silver | PK không null / không trùng; rule theo bảng (khoảng giá trị, enum, định dạng) — gồm cả bảng Olist và Clickstream |
| Gold | PK (surrogate key) không null / không trùng; FK phải resolve được (không rơi về `-1` unknown member); rule giá trị |

Một rule fail khi > 5 % dòng vi phạm (giá trị NULL tính là vi phạm). Chạy tay:

```bash
docker compose exec airflow bash -c "spark-submit --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.13:4.0.1,org.apache.hadoop:hadoop-aws:3.4.1,io.unitycatalog:unitycatalog-spark_4.0_2.13:0.6.0 \
  /opt/spark/app/jobs/ops/data_quality.py --layer silver --table olist_orders"
```

Kết quả mỗi lần chạy được ghi (append) vào Delta table `s3a://silver/_data_quality_log/` (cột `layer, table_name, check_name, passed, detail, checked_at`). Thêm/sửa rule trong `get_bronze_checks` / `get_silver_checks` / `get_gold_checks` (`spark/app/common/quality.py`).

## Cấu trúc code Spark (`spark/app`)

```
spark/app/
  common/                      # package dùng chung (import: from common.x import y)
    config.py                  # SparkSession, env, bucket
    tables.py                  # registry duy nhất: layer → bảng → path (catalog, DQ, vacuum đều đọc từ đây)
    transforms.py              # extract_cdc_latest, safe_to_timestamp, generate_surrogate_key
    writers.py                 # write_with_metrics
    catalog.py                 # đăng ký Unity Catalog + Trino
    metrics.py                 # pipeline metrics
    quality.py                 # framework + rule data quality
  jobs/                        # mỗi file = 1 spark-submit = 1 bảng (tên file = tên bảng)
    bronze/      kafka_to_bronze.py, register_tables.py
    silver/      olist/olist_*.py, clickstream/click_*.py, register_tables.py
    gold/        dimensions/dim_*.py, facts/fact_*.py, register_tables.py
    ops/         data_quality.py, vacuum_tables.py
  scripts/                     # tiện ích chạy tay, Airflow không dùng
```

Thêm bảng mới: viết job trong `jobs/<layer>/…`, thêm vào `common/tables.py` và rule trong `common/quality.py`, rồi thêm task vào DAG (đường dẫn qua `SILVER_JOBS` / `GOLD_JOBS` trong `airflow/dags/spark_submit_defaults.py`).

