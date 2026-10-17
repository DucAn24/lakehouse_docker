# lakehouse_docker — Docker Compose version

Standalone Docker Compose port of the K3s/Helm lakehouse in the parent repo. Same pipeline, same code conventions (see `../AGENTS.md` for Spark/Gold/SQL conventions); only the runtime differs.

- `docker-compose.yml` replaces `helm/lakehouse`. Service names = the old K8s service names (`postgres`, `kafka`, `connect`, `trino`, `spark-master`, `airflow`), so no hostnames in code changed. Object storage is SeaweedFS (`weed mini`, service `s3`, S3 API `http://s3:8333`, admin UI :23646; keys `S3_ACCESS_KEY/S3_SECRET_KEY` in `.env`, bucket `lakehouse` created from `S3_BUCKET` at startup). Clients only see a generic S3 endpoint + `AWS_*` keys, so swapping the backend touches the `s3` service and `tests/iceberg_lake.py` only.
- Table format + catalog: Apache Iceberg (spec v2) in one Apache Polaris REST catalog `lakehouse` (service `polaris`, :8181; metadata in Postgres db `polaris`), shared by Spark, Trino/dbt, catalog-api (PyIceberg) and host clients (`examples/`: DuckDB, PyIceberg). Spark 4.0.4 / `iceberg-spark-runtime-4.0_2.13` + `iceberg-aws-bundle` 1.11.0 (package list in `spark/Dockerfile`, `airflow/Dockerfile`, `spark_submit_defaults.py`: keep in sync). The Trino catalog is also named `lakehouse` (`trino/catalog/lakehouse.properties`), so `lakehouse.<layer>.<table>` works in every engine; there is no registration step.
- Polaris setup: `polaris-db-init` → `polaris-bootstrap` (root principal from `POLARIS_ROOT_CLIENT_ID/SECRET`) → `polaris` → `polaris-init` (`polaris/init.sh`, idempotent: catalog on `s3://lakehouse/`, grant, namespaces bronze/silver/gold/monitoring; the tests run it too). No credential vending (`stsUnavailable`, only `endpointInternal` set): every client uses its own S3 keys and endpoint.
- Core stack by default; `--profile apps` adds metabase, grafana, kafka-ui, fastapi, catalog-api, portal, polaris-console (:8088, built from apache/polaris-tools at a pinned commit).
- **Code is bind-mounted** (`spark/app`, `airflow/dags`, `dbt/`, `api/`, `catalog-api/`): edits apply without rebuild. Only Dockerfile / `requirements.txt` changes need `docker compose build <svc>`.
- Secrets live in `.env` (gitignored); `.env.example` must keep placeholders (`MIMO_API_KEY=YOUR_MIMO_API_KEY`).
- Code here is a copy of the parent repo's `spark/`, `airflow/`, `api/` — changes are NOT synced between the two.
- Ops scripts (all in root `scripts/`): `scripts/seed-data.ps1`, `scripts/register-connectors.ps1`. Windows host: `curl.exe`, PowerShell.

Chatbot (`api/core/chatbot.py`, `--profile apps`): the schema in the system prompt is built at runtime by `core/schema_context.py` (Trino `DESCRIBE` + descriptions/FKs from `dbt/models/*.yml`, mounted read-only at `/dbt/models`; cached 10 min), so a new gold model needs no prompt edit. `run_select_query` validates with `EXPLAIN (TYPE VALIDATE)` and returns Trino's error plus the real columns so the model can self-correct (3 failures per request, then it stops). `/chat` returns `queries` (sql, rows, chart hint from `core/charts.py`) next to the text; the UI renders SQL/chart/table from them.

Spark code layout (`spark/app`, on `PYTHONPATH`):

- `common/` shared package: `config` (session/env; `iceberg_catalog_conf()` = Polaris REST, or a file-based catalog when `ICEBERG_CATALOG_TYPE=hadoop`), `tables` (single table registry, layer → table → identifier `lakehouse.<layer>.<table>`, `table_id()`), `transforms`, `writers`, `catalog` (`create_or_replace`, `append_rows`, `trino_execute`), `metrics`, `quality` (DQ rules).
- Tables are always addressed by identifier (`spark.table(...)`, `writeTo(...)`), never by path.
- `jobs/bronze/`, `jobs/silver/{olist,clickstream}/`: one spark-submit entry point per table, file name = table name.
- `jobs/ops/`: `data_quality.py --layer …`, `maintain_tables.py` (Iceberg `expire_snapshots` / `remove_orphan_files` / `rewrite_manifests` on every table; task `maintain_iceberg_tables`). `lakehouse.monitoring.pipeline_metrics` + `data_quality_log` feed the Grafana *Pipeline Monitoring* dashboard/alerts. `jobs/ops/show_customers.py` is ad-hoc, not run by Airflow. Postgres seed SQL: `postgres/sql/`.
- Silver jobs write with `upsert_with_metrics(..., table_id("silver", t), SILVER_KEYS[t])`: a SQL MERGE whose insert/update/delete counts are computed just before it; one snapshot per run, with row changes via `read_changes` (Iceberg changelog view). New silver table → also add its key to `SILVER_KEYS`.
- New silver table → add the job, add it to `common/tables.py` and the rules in `common/quality.py`, add the task in the DAG (paths via `SILVER_JOBS` in `spark_submit_defaults.py`).
- Import shared code as `from common.x import y`. No Python UDFs today, so executors never import project code; if you add one, ship `common/` with `--py-files`.

Gold layer = dbt on Trino (`dbt/`, dbt-trino; pins in `dbt/requirements.txt`, installed in a separate venv `/opt/dbt-venv` in the airflow image):

- `models/{dimensions,facts}/<table>.sql`, file name = gold table name; sources = `lakehouse.silver.*` (`models/sources.yml`). Tests + docs in `_dimensions.yml` / `_facts.yml`; custom generic tests in `tests/generic/` (`unknown_member_ratio` = the 5 % unknown-FK tolerance).
- Every model starts with `{{ config(properties=gold_properties()) }}` (Iceberg `format_version = 2`; Polaris places the table under `s3://lakehouse/gold/`); `on_table_exists: replace` → `CREATE OR REPLACE TABLE`, a new Iceberg snapshot of the same table. Spark then reads gold from the same catalog (gold DQ gate, maintenance).
- Surrogate keys: `hash_key([...])` (xxhash64 of the natural key, stable across rebuilds); each dimension has an UNKNOWN member `-1`, facts `coalesce(..., -1)`. Date keys: `date_key(ts)` → yyyyMMdd.
- DAG: `dbt_build` (`airflow/dags/gold_tasks.py`, `dbt build` = models + tests) → `dq_gold` (→ `maintain_iceberg_tables` in the full pipeline). The `on-run-end` hook `record_run_metrics` writes each model run to `monitoring.pipeline_metrics`.
- New gold table → add the model (+ tests in the yml), add it to `common/tables.py` and the rules in `get_gold_checks()` (`check_spark_layout.py` compares models vs registry).

Verify:

```bash
docker compose config -q
ruff check api/ airflow/dags/ spark/app/ scripts/ catalog-api/
python scripts/check_spark_layout.py   # imports, DAG job paths, table registry vs DQ rules
find spark/app -name '*.py' -exec python -m py_compile {} +
pip install -r tests/requirements.txt && python -m pytest tests   # needs JDK 17+ (Spark) and Docker (dbt gold); those tests skip otherwise
```

Tests (`tests/`): registry/layout (no Spark), `common/` units, DQ rules, DAG structure (skipped without Airflow) and an end-to-end run (`test_pipeline_e2e.py`: tiny Debezium bronze fixtures in `bronze_fixtures.py` → every silver job → `dbt build` of gold → DQ gates) on a local Spark session (real `create_spark_session`) against throwaway Polaris + SeaweedFS (S3) containers, with Trino added on demand for dbt (`iceberg_lake.py`, runs `polaris/init.sh`); Spark and Trino share the catalog, nothing is copied. Without Docker, Spark tests use a file-based Iceberg catalog in a temp dir (`ICEBERG_CATALOG_TYPE=hadoop`) and gold tests skip. In a container, set `LAKE_HOST=host.docker.internal`. New table → add a fixture to `bronze_fixtures.py` (the e2e test fails if a bronze table has none).
