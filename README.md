# 🌊 Production-Grade Lakehouse Platform (Docker Compose Edition)

[![Apache Spark](https://img.shields.io/badge/Apache_Spark-4.0.4-E25A1C?logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-4.0.1-003366?logo=delta&logoColor=white)](https://delta.io/)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog_OSS-0.6.0-00A4E4?logo=databricks&logoColor=white)](https://unitycatalog.io/)
[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-3.2.1-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Trino](https://img.shields.io/badge/Trino-483-DD00A1?logo=trino&logoColor=white)](https://trino.io/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-7.6.0_KRaft-231F20?logo=apache-kafka&logoColor=white)](https://kafka.apache.org/)
[![Debezium](https://img.shields.io/badge/Debezium_CDC-2.7.0-93005A)](https://debezium.io/)
[![MinIO](https://img.shields.io/badge/MinIO-S3_Storage-C72C48?logo=minio&logoColor=white)](https://min.io/)
[![Grafana](https://img.shields.io/badge/Grafana-11.x-F46800?logo=grafana&logoColor=white)](https://grafana.com/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Docker Compose](https://img.shields.io/badge/Docker_Compose-v2+-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white)](https://www.python.org/)

An enterprise-grade, local **Medallion Lakehouse platform** (Bronze → Silver → Gold) engineered for high-throughput **Change Data Capture (CDC)**, transactional table management, automated data governance, and full-stack analytics.

This repository provides an autonomous, developer-friendly **Docker Compose** implementation—completely eliminating Kubernetes control-plane overhead while preserving a cloud-native architecture, production-grade quality gates, dual-catalog governance, and end-to-end pipeline observability.

---

## 📑 Table of Contents

- [System Architecture](#-system-architecture)
- [Key Features](#-key-features)
- [Medallion Data Architecture](#-medallion-data-architecture)
  - [Layer Design & Table Registry](#layer-design--table-registry)
  - [Dimensional Data Modeling (Gold Layer)](#dimensional-data-modeling-gold-layer)
- [Core Engineering Deep Dives](#-core-engineering-deep-dives)
  - [1. Incremental Silver Upserts & Change Data Feed (CDF)](#1-incremental-silver-upserts--change-data-feed-cdf)
  - [2. Multi-Layer Data Quality Gates](#2-multi-layer-data-quality-gates)
  - [3. Dual-Catalog Architecture & STS Credential Vending](#3-dual-catalog-architecture--sts-credential-vending)
  - [4. Observability & Webhook Alerting](#4-observability--webhook-alerting)
  - [5. Natural Language Analytics Chatbot](#5-natural-language-analytics-chatbot)
- [Services & Port Matrix](#-services--port-matrix)
- [Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [Quick Start in 5 Minutes](#quick-start-in-5-minutes)
  - [Enabling the BI & Application Tier](#enabling-the-bi--application-tier)
- [Developer Workflow & Operations](#-developer-workflow--operations)
  - [Live Code Bind-Mounts](#live-code-bind-mounts)
  - [Common Docker Compose Commands](#common-docker-compose-commands)
  - [Adding a New Table to the Lakehouse](#adding-a-new-table-to-the-lakehouse)
  - [Static Layout & Quality Gate Checks](#static-layout--quality-gate-checks)
  - [Automated Testing Suite](#automated-testing-suite)
- [CI/CD & Container Image Publishing](#-cicd--container-image-publishing)
- [Project Directory Layout](#-project-directory-layout)

---

## 🏗️ System Architecture

```mermaid
flowchart TB
    subgraph DataTier["01. OLTP & Ingestion Tier"]
        PG[("PostgreSQL\n(OLTP Orders & Clickstream)\nwal_level=logical")]
        DEB["Debezium Connect 2.7\n(PostgreSQL CDC)"]
        KAFKA{{"Apache Kafka 7.6\n(KRaft Mode)\n16 CDC Topics"}}
        PG -->|"WAL Changes"| DEB
        DEB -->|"JSON CDC Events"| KAFKA
    end

    subgraph SparkProcessing["02. Spark Processing Tier (spark://spark-master:7077)"]
        K2B["kafka_to_bronze.py\n(Raw Ingestion)"]
        B2S["Silver Domain Jobs\n(olist_*.py & click_*.py)\nMERGE + Change Data Feed"]
    end

    subgraph DbtModels["dbt on Trino (dbt/)"]
        S2G["Gold dbt Models\n(dim_*.sql & fact_*.sql)\nStar Schema + Data Tests"]
    end

    subgraph LakehouseStorage["03. Storage & Governance Tier (MinIO S3)"]
        BRONZE[("s3://bronze\nRaw CDC Delta Tables")]
        SILVER[("s3://silver\nCleaned Domain Entities\n+ _pipeline_metrics\n+ _data_quality_log")]
        GOLD[("s3://gold\nDimensions & Fact Tables")]
        METASTORE[("s3://trino-metastore\nFile Metastore for Trino")]
        UC[("Unity Catalog OSS 0.6\nCatalog: lakehouse\nSTS AssumeRole Auth")]
    end

    subgraph QualityGates["04. Data Quality & Airflow Orchestration"]
        AF["Apache Airflow 3.x\n(LocalExecutor)\nlakehouse_etl_pipeline"]
        DQB{"dq_bronze\nEnvelope & Key"}
        DQS{"dq_silver\nPK Uniqueness & Domain"}
        DQG{"dq_gold\nFK & Referential Integrity"}
        REGM["register_monitoring\n(delta.monitoring.*)"]
    end

    subgraph ServingTier["05. Query, Serving & Application Tier (profile: apps)"]
        TRINO["Trino 483\n(Distributed SQL Engine)"]
        GRAFANA["Grafana 11\n(Lakehouse Overview &\nPipeline Monitoring)"]
        METABASE["Metabase BI\n(Self-Service Dashboards)"]
        KAFKAUI["Kafka UI\n(Topic & Connector Inspector)"]
        FASTAPI["FastAPI / MiMo AI\n(Text-to-SQL Analytics Chatbot)"]
        WEBHOOK[/"Slack / Discord Alert Webhook"\]
    end

    %% Ingestion Flow
    KAFKA -->|"Batch Consume"| K2B
    K2B -->|"Write Raw Delta"| BRONZE
    BRONZE --> DQB
    DQB -->|"Gate Passed"| B2S
    B2S -->|"Incremental MERGE"| SILVER
    SILVER --> DQS
    DQS -->|"Gate Passed"| S2G
    S2G -->|"Write Star Schema"| GOLD
    GOLD --> DQG
    DQG --> REGM

    %% Catalog registrations
    BRONZE -.->|"External Table Registration"| UC
    SILVER -.->|"External Table Registration"| UC
    GOLD -.->|"External Table Registration"| UC
    BRONZE -.->|"delta.system.register_table"| METASTORE
    SILVER -.->|"delta.system.register_table"| METASTORE
    GOLD -.->|"delta.system.register_table"| METASTORE

    %% Serving
    METASTORE -.-> TRINO
    GOLD -.-> TRINO
    SILVER -.-> TRINO
    TRINO --> METABASE
    TRINO --> GRAFANA
    TRINO --> FASTAPI
    KAFKA --> KAFKAUI

    %% Orchestration & Alerting
    AF -->|"Triggers & Gates"| SparkProcessing
    AF -.->|"Task Failures"| WEBHOOK
    GRAFANA -.->|"Alert Rules (>26h stale, DQ fail)"| WEBHOOK
```

---

## ⚡ Key Features

| Capability | Engineering Highlight |
|---|---|
| **Zero-Friction Local Dev** | Source code (`spark/app`, `airflow/dags`, `api/`) is bind-mounted directly. Edit code on your host, run tasks immediately without rebuilding Docker images. |
| **Full Medallion Pipeline** | Bronze (raw CDC envelope), Silver (cleaned, normalized domain models), and Gold (Kimball star schema dimensional marts). |
| **Incremental Silver MERGE** | Business-key upsert with **Delta Change Data Feed (CDF)** enabled. Unchanged records are untouched (`processed_at` preserved), while insertions, updates, and deletions emit audit feeds. |
| **Multi-Stage Data Quality Gates** | Airflow execution halts if data quality fails (`dq_bronze`, `dq_silver`, `dq_gold`). Results are logged to an audit Delta table with strict failure thresholds (`> 5%` violation rate). |
| **Dual-Catalog Governance** | Spark executes queries via **Unity Catalog OSS 0.6.0** (with MinIO STS credential vending), while Trino serves analytical queries via a dedicated S3 file metastore. |
| **Enterprise Observability** | Automatic capture of row-level write statistics (`rows_inserted`, `rows_updated`, `rows_deleted`) and DQ logs exposed to Grafana and Trino under `delta.monitoring.*`. |
| **Proactive Alerting** | Slack/Discord webhook alerts triggered on Airflow task failures and Grafana alerting rules (stale tables > 26h, failed jobs, DQ rule breaches). |
| **Generative AI Chatbot** | FastAPI app with Xiaomi MiMo LLM (`mimo-v2.5`), enabling conversational Text-to-SQL queries directly against Trino Gold tables. |

---

## 📊 Medallion Data Architecture

### Layer Design & Table Registry

Every table in the platform is managed through a single central registry in [`spark/app/common/tables.py`](file:///d:/Projects/lakehouse_docker/spark/app/common/tables.py), ensuring catalog registration, DQ validation, and data maintenance never drift apart.

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                       MEDALLION DATA FLOW                                              │
├──────────────────────────┬─────────────────────────────────────┬───────────────────────────────────────┤
│       BRONZE LAYER       │            SILVER LAYER             │              GOLD LAYER               │
│      Raw CDC Logs        │      Cleaned Domain Entities        │       Dimensional Marts (Star)        │
├──────────────────────────┼─────────────────────────────────────┼───────────────────────────────────────┤
│ • olist_orders           │ • olist_orders                      │ • dim_date                            │
│ • olist_customers        │ • olist_customers                   │ • dim_customer                        │
│ • olist_geolocation      │ • olist_sellers                     │ • dim_seller                          │
│ • olist_order_items      │ • olist_products                    │ • dim_product                         │
│ • olist_order_payments   │ • olist_order_items                 │ • dim_device                          │
│ • olist_order_reviews    │ • olist_order_payments              │ • dim_traffic_source                  │
│ • olist_products         │ • olist_order_reviews               │ • dim_click_customer                  │
│ • olist_sellers          │ • click_customers                   │ • dim_click_product                   │
│ • product_category_trans │ • click_products                    │ • fact_orders                         │
│ • click_customers        │ • click_sessions                    │ • fact_reviews                        │
│ • click_products         │ • click_orders                      │ • fact_click_orders                   │
│ • click_sessions         │ • click_order_items                 │ • fact_sessions                       │
│ • click_orders           │ • click_events                      │ • fact_clickstream_events             │
│ • click_order_items      │ • click_reviews                     │ • fact_click_reviews                  │
│ • click_events           │                                     │                                       │
│ • click_reviews          │                                     │                                       │
├──────────────────────────┼─────────────────────────────────────┼───────────────────────────────────────┤
│ Storage: s3://bronze/    │ Storage: s3://silver/               │ Storage: s3://gold/                   │
│ Format: Delta Lake       │ Format: Delta Lake + CDF            │ Format: Delta Lake                    │
│ Mode: Kafka CDC Snapshot │ Mode: MERGE Upsert (SILVER_KEYS)    │ Mode: Full Dimensional Rebuild        │
└──────────────────────────┴─────────────────────────────────────┴───────────────────────────────────────┘
```

### Dimensional Data Modeling (Gold Layer)

The Gold layer transforms domain entities into high-performance Kimball Star Schemas:
- **Built with dbt on Trino**: Every dimension and fact is a dbt model in [`dbt/models/`](dbt/models/) (`dbt-trino`), written as `CREATE OR REPLACE TABLE` at its fixed `s3://gold/<table>` location, so each run is a new Delta version (time travel preserved). `dbt build` runs the models in dependency order together with their data tests (unique / not-null keys, FK relationships, accepted values, unknown-member ratio).
- **Surrogate Keys**: Deterministic `BIGINT` keys (`hash_key` macro: `xxhash64` over the natural business key), stable across full rebuilds.
- **Handling Unknown / Late-Arriving Dimensions**: Failed foreign key lookups gracefully resolve to standard unknown member records (`-1`), preventing query joins from silently dropping metric values.
- **Analytical Use Cases**:
  - `fact_orders`: Order sales metrics, freight costs, payment counts, approval-to-delivery durations.
  - `fact_sessions` & `fact_clickstream_events`: Funnel conversion analysis, bounce rates, session durations, device and traffic attribution.
  - `fact_reviews`: Customer sentiment trends, fulfillment satisfaction, review scores by seller and product category.

---

## 🔍 Core Engineering Deep Dives

### 1. Incremental Silver Upserts & Change Data Feed (CDF)

Instead of expensive full-table overwrites, Silver tables are updated via `upsert_with_metrics` ([`spark/app/common/writers.py`](file:///d:/Projects/lakehouse_docker/spark/app/common/writers.py)):

1. **MERGE Strategy**: Records are matched against existing Delta tables using the designated business key (`SILVER_KEYS`):
   - **`WHEN MATCHED AND source._op = 'd'`** $\rightarrow$ `DELETE`
   - **`WHEN MATCHED AND (source != target)`** $\rightarrow$ `UPDATE SET *`
   - **`WHEN NOT MATCHED`** $\rightarrow$ `INSERT *`
2. **Timestamp Preservation**: If attributes are identical, records are **not rewritten**, preserving the original `processed_at` timestamp as a true "last modified" audit attribute.
3. **Delta Change Data Feed**: Every Silver table has `spark.databricks.delta.changeDataFeed.enabled = true`. Downstream consumers invoke `read_changes(spark, path, starting_version)` to retrieve exact row change actions (`insert`, `update_postimage`, `delete`), commit versions, and timestamps.
4. **Metrics Instrumentation**: Each execution appends execution records into `s3a://silver/_pipeline_metrics/` tracking `rows_inserted`, `rows_updated`, `rows_deleted`, `write_mode`, and duration.

### 2. Multi-Layer Data Quality Gates

Data Quality is enforced as blocking DAG gates (`jobs/ops/data_quality.py`). If more than **5%** of records in a table violate any assertion rule, the job exits with code `1`, immediately halting downstream stages.

```
Airflow DAG Execution
 ├── Kafka to Bronze
 │    └── Register Bronze Tables
 │         └── [GATE: dq_bronze] ────► Fails if envelope malformed or keys null
 ├── Bronze to Silver (14 Parallel Jobs)
 │    └── Register Silver Tables
 │         └── [GATE: dq_silver] ────► Fails on duplicate PKs or domain enum violations
 └── Silver to Gold (dbt build: 14 models + data tests on Trino)
      └── Register Gold Tables
           └── [GATE: dq_gold] ──────► Fails on unresolved FKs (>5% fallback to -1)
                └── register_monitoring (all_done) ──► Publishes metrics & DQ logs to Trino
```

- **Bronze Checks**: CDC envelope verification (`op` in `c, r, u, d`, valid timestamps `ts_ms > 0`), valid payload fields (`after` not null unless delete), non-null business identifiers.
- **Silver Checks**: Primary key uniqueness and non-nullability; domain-specific bounds (e.g. `price > 0`, valid geolocation ranges, review scores `[1, 5]`, allowed order statuses).
- **Gold Checks**: Surrogate key integrity, non-negative fact metrics, and referential integrity (foreign key joins must resolve without excessive fallback to `-1`).
- **Audit Log**: Every check result is appended to `s3a://silver/_data_quality_log/` and exposed to Trino as `delta.monitoring.data_quality_log`.

### 3. Dual-Catalog Architecture & STS Credential Vending

To balance cutting-edge catalog governance with robust distributed SQL querying:

1. **Apache Spark + Unity Catalog OSS 0.6.0**:
   - Replaces legacy Hive Metastore with standard Unity Catalog (`lakehouse` catalog).
   - Metastore state persists in an embedded H2 database within Docker volume `uc-data`.
   - Spark communicates with UC via REST endpoints (`http://unitycatalog:8080/api/2.1/unity-catalog`).
   - Unity Catalog performs **credential vending** by requesting temporary STS tokens from MinIO (`AWS_ENDPOINT_URL_STS`) using a dedicated service user (`UC_S3_ACCESS_KEY`).
   - Tables are registered as external tables pointing to `s3://<bucket>/<table>/`.
2. **Trino + Delta File Metastore**:
   - Trino does not yet support Unity Catalog OSS Delta protocols natively.
   - The platform registers every table concurrently into Trino's Delta catalog using an S3 file metastore (`s3://trino-metastore/`) via `CALL delta.system.register_table(...)`.
   - Analysts query tables seamlessly as `delta.gold.<table_name>` or `delta.monitoring.<table_name>`.

### 4. Observability & Webhook Alerting

The platform includes production-ready monitoring and alerting provisioned out of the box:

- **Pipeline Monitoring Dashboard** (Grafana, `http://localhost:3001`):
  - **Freshness Gauge**: Flags tables where last successful commit is older than 26 hours.
  - **Execution Trends**: Tracks step-by-step job runtime duration across layers.
  - **MERGE Activity**: Visualizes rows inserted vs. updated vs. deleted over time.
  - **Data Quality**: Real-time pass/fail rates, assertion breaches, and error summaries.
- **Lakehouse Business Overview Dashboard**:
  - E-Commerce revenue by Brazilian state, category performance, order fulfillment lead time.
  - Clickstream conversion funnels, bounce rates, device and traffic distribution.
- **Webhook Alerting Integration**:
  - Configure `ALERT_WEBHOOK_URL` in `.env` (compatible with Slack, Discord `/slack`, Mattermost).
  - Airflow triggers instant notifications on task failure via [`airflow/dags/alerting.py`](file:///d:/Projects/lakehouse_docker/airflow/dags/alerting.py).
  - Grafana alert rules actively monitor table freshness and DQ breach thresholds.

### 5. Natural Language Analytics Chatbot

An integrated AI assistant running under the `apps` profile:
- Built with **FastAPI** and styled with modern dark-mode UI ([`api/`](file:///d:/Projects/lakehouse_docker/api)).
- Powered by **Xiaomi MiMo LLM** (`mimo-v2.5` via OpenAI-compatible API).
- Translates natural language questions into valid Trino SQL targeting `delta.gold.*` tables.
- Validates queries, enforces `SELECT`-only execution, and formats executive business summaries with tabular data.

---

## 🖥️ Services & Port Matrix

| Service | Host Port | Internal Port | Purpose | Default Credentials |
|---|---|---|---|---|
| **Airflow Webserver** | [localhost:8084](http://localhost:8084) | `8080` | DAG orchestration & pipeline tracking | `airflow` / `airflow` |
| **Spark Master UI** | [localhost:8080](http://localhost:8080) | `8080` | Spark cluster & job execution status | — |
| **Trino Coordinator** | [localhost:8081](http://localhost:8081) | `8080` | Distributed SQL query engine | `admin` (no password) |
| **Unity Catalog API** | [localhost:8086](http://localhost:8086) | `8080` | Open data catalog REST API | — |
| **Kafka Connect API** | [localhost:8083](http://localhost:8083) | `8083` | Debezium CDC connectors manager | — |
| **MinIO Console** | [localhost:9001](http://localhost:9001) | `9001` | S3 bucket & object browser | `admin` / `password123` |
| **MinIO S3 API** | `localhost:9000` | `9000` | S3 object storage gateway | `admin` / `password123` |
| **PostgreSQL** | `localhost:5432` | `5432` | OLTP source database (`orders`) | `postgres` / `123456` |
| **Apache Kafka** | `localhost:29092` | `9092` | KRaft distributed event broker | — |
| **Metabase** *(apps)* | [localhost:3000](http://localhost:3000) | `3000` | Business intelligence & visualizations | Initial setup on first visit |
| **Grafana** *(apps)* | [localhost:3001](http://localhost:3001) | `3000` | Pre-provisioned dashboards & alerts | `admin` / `admin` |
| **Kafka UI** *(apps)* | [localhost:8085](http://localhost:8085) | `8080` | Topic message & consumer inspector | — |
| **FastAPI / Chatbot** *(apps)* | [localhost:8000](http://localhost:8000) | `8000` | SQL API & MiMo AI query assistant | — |

---

## 🚀 Getting Started

### Prerequisites

- **Docker Desktop** with **WSL2** backend enabled (Windows) or native Docker Engine (Linux/macOS).
- **RAM Allocation**: Minimum **8 GB RAM** assigned to Docker/WSL2 (core pipeline requires ~7–8 GB; adding `--profile apps` requires ~10–11 GB).
- **PowerShell** (Windows) or **Bash** (Linux/macOS).
- Git.

### Quick Start in 5 Minutes

#### 1. Clone & Configure Environment

```powershell
git clone https://github.com/DucAn24/lakehouse_docker.git
cd lakehouse_docker

# Create local environment configuration
Copy-Item .env.example .env
```

> [!TIP]
> Edit `.env` to customize memory allocations, dataset paths, or supply your `MIMO_API_KEY` for the AI chatbot and `ALERT_WEBHOOK_URL` for Slack/Discord alerts.

#### 2. Build & Launch the Core Lakehouse Stack

```powershell
# Build Spark, Airflow, and API images locally
docker compose build

# Start the core ETL infrastructure
docker compose up -d

# Verify services reach 'healthy' state (init containers will show 'Exited (0)')
docker compose ps
```

#### 3. Seed OLTP Database (PostgreSQL CDC)

Mounts and imports the Brazilian E-Commerce (Olist) and Clickstream datasets into PostgreSQL. Debezium instantly captures WAL events and streams them into Kafka:

```powershell
.\scripts\seed-data.ps1
```

#### 4. Trigger the Airflow Pipeline

1. Open Airflow at **[http://localhost:8084](http://localhost:8084)** (`airflow` / `airflow`).
2. Unpause and trigger the DAG **`lakehouse_etl_pipeline`**.
3. Watch the pipeline advance through:
   - `bronze_ingestion` $\rightarrow$ `register_bronze_tables` $\rightarrow$ `dq_bronze`
   - `bronze_to_silver` (parallel domain entities) $\rightarrow$ `register_silver_tables` $\rightarrow$ `dq_silver`
   - `silver_to_gold`: `dbt_build` (star schema models + tests) $\rightarrow$ `register_gold_tables` $\rightarrow$ `dq_gold`
   - `finish_pipeline` (`register_monitoring` $\rightarrow$ `pipeline_done`)

#### 5. Verify via Spark / Trino

Execute ad-hoc Trino queries directly via the Trino CLI or Web UI:

```powershell
# Query Gold Dimensions via Trino
docker compose exec trino trino --catalog delta --schema gold --execute "SELECT * FROM dim_customer LIMIT 5;"

# Query Pipeline Metrics
docker compose exec trino trino --catalog delta --schema monitoring --execute "SELECT layer, table_name, write_mode, rows_inserted, rows_updated, duration_seconds FROM pipeline_metrics ORDER BY recorded_at DESC LIMIT 10;"
```

---

### Enabling the BI & Application Tier

To launch Metabase, Grafana, Kafka UI, and the FastAPI Chatbot:

```powershell
# Start all application services
docker compose --profile apps up -d

# Check applications status
docker compose --profile apps ps

# To free up RAM during heavy Spark processing, stop apps anytime:
docker compose --profile apps stop metabase grafana kafka-ui fastapi
```

Access Grafana at **[http://localhost:3001](http://localhost:3001)** (`admin` / `admin`) and explore the **Lakehouse Overview** and **Pipeline Monitoring** dashboards under the *Lakehouse* folder!

---

## 🛠️ Developer Workflow & Operations

### Live Code Bind-Mounts

All application code is mounted directly into running containers:

```
Host Filesystem                      Container Path                  Reload Mechanism
─────────────────────────────────────────────────────────────────────────────────────────────
./spark/app                 ──►     /opt/spark/app                  Next spark-submit task
./airflow/dags              ──►     /opt/airflow/dags               Airflow scheduler auto-refresh
./api                       ──►     /app                            uvicorn --reload (instant)
./grafana/dashboards        ──►     /var/lib/grafana/dashboards     Grafana auto-reloads in ~30s
```

> [!NOTE]
> Modifying Spark transformations, Airflow DAG definitions, or FastAPI endpoints **never requires rebuilding container images**. Simply edit and run!

### Common Docker Compose Commands

```powershell
# Stream logs for Airflow scheduler or Spark master
docker compose logs -f airflow
docker compose logs -f spark-master

# Connect to PostgreSQL directly
docker compose exec postgres psql -U postgres -d orders

# Re-register Debezium Kafka connectors if re-created
.\scripts\register-connectors.ps1

# Gracefully stop containers (preserves all data volumes)
docker compose down

# Stop and wipe ALL data volumes (clean slate)
docker compose down -v
```

### Adding a New Table to the Lakehouse

Follow this 5-step checklist to introduce a new table:

1. **Create the Job / Model**:
   - For Silver: Write [`spark/app/jobs/silver/<domain>/<table_name>.py`](file:///d:/Projects/lakehouse_docker/spark/app/jobs/silver/). Use `upsert_with_metrics(..., SILVER_KEYS[table])`.
   - For Gold: Write the dbt model [`dbt/models/<dimensions|facts>/<table_name>.sql`](dbt/models/) starting with `{{ config(properties=gold_properties()) }}`, and declare its tests in `_dimensions.yml` / `_facts.yml`. No Airflow change needed: `dbt_build` picks it up.
2. **Register in Table Registry**:
   - Add the table name and Delta path to [`spark/app/common/tables.py`](file:///d:/Projects/lakehouse_docker/spark/app/common/tables.py).
   - If Silver, add its primary business key to `SILVER_KEYS`.
3. **Define Data Quality Rules**:
   - Add assertion rules into `get_silver_checks()` or `get_gold_checks()` in [`spark/app/common/quality.py`](file:///d:/Projects/lakehouse_docker/spark/app/common/quality.py).
4. **Wire Airflow Task** (Silver only):
   - Add the task entry point to [`airflow/dags/spark_submit_defaults.py`](file:///d:/Projects/lakehouse_docker/airflow/dags/spark_submit_defaults.py) and incorporate it into the appropriate stage of [`airflow/dags/lakehouse_etl_pipeline.py`](file:///d:/Projects/lakehouse_docker/airflow/dags/lakehouse_etl_pipeline.py).
5. **Add E2E Bronze Fixture**:
   - Add sample Debezium CDC records in [`tests/bronze_fixtures.py`](file:///d:/Projects/lakehouse_docker/tests/bronze_fixtures.py) so automated tests validate the pipeline.

### Static Layout & Quality Gate Checks

Validate repository structure, DAG imports, and Spark layout without starting containers:

```powershell
# Run Ruff linting
ruff check api/ airflow/dags/ spark/app/ scripts/

# Verify Spark imports, DAG paths, and table registries
python scripts/check_spark_layout.py

# Verify Airflow DAG parsing (checks syntax & missing files)
python scripts/check_dags.py

# Byte-compile Spark Python files
Get-ChildItem -Path spark/app -Filter *.py -Recurse | ForEach-Object { python -m py_compile $_.FullName }
```

### Automated Testing Suite

The repository includes a comprehensive `pytest` test suite:

```powershell
# Install test requirements
pip install -r tests/requirements.txt

# Run all unit and validation tests
python -m pytest tests -v
```

- **`test_registry.py`**: Validates table consistency across paths and layers.
- **`test_transforms.py`**: Tests CDC deduplication, timestamp coercion, and the metrics writers.
- **`test_upsert.py`**: Validates Delta MERGE logic, schema evolution, and Change Data Feed operations.
- **`test_quality.py`**: Tests DQ assertions, failure thresholds, and metric logging.
- **`test_dags.py`**: Ensures Airflow DAG DAG-integrity and operator parameters.
- **`test_monitoring_assets.py`**: Validates Grafana dashboards, alert provisioning, and Trino metrics.
- **`test_pipeline_e2e.py`**: Runs an end-to-end integration test (Bronze fixtures $\rightarrow$ Silver MERGE $\rightarrow$ `dbt build` of the Gold star schema $\rightarrow$ DQ Gates) on an ephemeral Spark + Delta engine; the dbt step runs on throwaway Trino + MinIO containers (needs Docker).

---

## 🔄 CI/CD & Container Image Publishing

The project uses GitHub Actions workflows configured in [`.github/workflows/`](file:///d:/Projects/lakehouse_docker/.github/workflows/):

| Workflow | Trigger | Description |
|---|---|---|
| **CI (`ci.yml`)** | Pull Requests, push to `dev`, manual dispatch | Runs `ruff`, `py_compile`, `check_spark_layout.py`, Airflow DAG validation, `docker compose config -q`, and builds container images with GitHub Actions caching. |
| **CD (`cd.yml`)** | Push to `master`, git tags (`v*`), manual dispatch | Executes CI validation, then builds and publishes multi-architecture images to GitHub Container Registry (**GHCR**): `ghcr.io/<owner>/lakehouse-{spark,airflow,fastapi}`. |

### Running with Published GHCR Images

To deploy on a remote server using pre-built images without building locally:

```powershell
# In .env, uncomment and configure image tags:
SPARK_IMAGE=ghcr.io/<owner>/lakehouse-spark:latest
AIRFLOW_IMAGE=ghcr.io/<owner>/lakehouse-airflow:latest
FASTAPI_IMAGE=ghcr.io/<owner>/lakehouse-fastapi:latest

# Pull and start without local build
docker compose pull
docker compose up -d --no-build
```

---

## 📂 Project Directory Layout

```
lakehouse_docker/
├── .github/
│   └── workflows/             # GitHub Actions CI/CD workflows (ci.yml, cd.yml)
├── airflow/
│   ├── Dockerfile             # Custom Airflow image with Spark binaries & providers
│   └── dags/                  # Airflow DAGs (medallion stages, monitoring, alerts)
├── api/
│   ├── Dockerfile             # FastAPI container image
│   ├── app.py                 # FastAPI application with MiMo AI Chatbot
│   ├── core/                  # Trino client & Chatbot LLM orchestrator
│   └── static/                # Modern dark-mode web interface
├── connectors/                # Debezium PostgreSQL connector definitions (.json)
├── dbt/                       # Gold layer: dbt-trino project (models, tests, macros), bound to /opt/airflow/dbt
├── grafana/
│   ├── dashboards/            # Pre-provisioned Grafana dashboards (.json)
│   └── provisioning/          # Datasource, dashboard & alert rule provisioning
├── postgres/
│   └── init-databases.sh      # Initializes orders, airflow, metabase databases
├── scripts/
│   ├── check_dags.py          # Verifies Airflow DAG structure and imports
│   ├── check_spark_layout.py  # Verifies registry, DQ rules, and file layout
│   ├── register-connectors.ps1# Helper script to post Debezium connectors
│   └── seed-data.ps1          # Populates PostgreSQL with Olist & Clickstream data
├── spark/
│   ├── Dockerfile             # Spark 4.0.4 + Delta 4.0.1 + Unity Catalog image
│   └── app/                   # Bound to /opt/spark/app (Live code)
│       ├── common/            # Shared library: config, tables, transforms, writers, DQ
│       └── jobs/              # Individual Spark entrypoints (bronze, silver, ops) + catalog registration
├── tests/                     # Unit, integration, and E2E PySpark tests
├── trino/                     # Trino configuration and Delta Lake catalog properties
├── unitycatalog/              # Unity Catalog server configuration (server.properties)
├── .env.example               # Environment variable template with default settings
├── docker-compose.yml         # Primary orchestration manifest (12 services)
└── README.md                  # System documentation
```

---

## ⚖️ License & Attribution

This project is open-source and available under the [MIT License](LICENSE). 
Datasets used include the Brazilian E-Commerce Public Dataset by [Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and synthetic clickstream event logs.
