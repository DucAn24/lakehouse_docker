# lakehouse_docker — Docker Compose version

Standalone Docker Compose port of the K3s/Helm lakehouse in the parent repo. Same pipeline, same code conventions (see `../AGENTS.md` for Spark/Gold/SQL conventions); only the runtime differs.

- `docker-compose.yml` replaces `helm/lakehouse`. Service names = the old K8s service names (`postgres`, `minio`, `kafka`, `connect`, `trino`, `spark-master`, `airflow`), so no hostnames in code changed.
- Catalog: Unity Catalog OSS (`unitycatalog`, catalog `lakehouse`) replaced Hive Metastore. Spark 4.0.4 / Delta 4.0.1 / `unitycatalog-spark_4.0_2.13:0.6.0`. Trino can't read Delta from UC OSS, so its `delta` catalog uses a file metastore on `s3://trino-metastore/`; `common.catalog.register_catalog_tables` registers every table in both. UC only accepts `s3://` locations (not `s3a://`).
- Core stack by default; `--profile apps` adds metabase, grafana, kafka-ui, fastapi.
- **Code is bind-mounted** (`spark/app`, `airflow/dags`, `api/`): edits apply without rebuild. Only Dockerfile / `requirements.txt` changes need `docker compose build <svc>`.
- Secrets live in `.env` (gitignored); `.env.example` must keep placeholders (`MIMO_API_KEY=YOUR_MIMO_API_KEY`).
- Code here is a copy of the parent repo's `spark/`, `airflow/`, `api/` — changes are NOT synced between the two.
- Ops scripts: `scripts/seed-data.ps1`, `scripts/register-connectors.ps1`. Windows host: `curl.exe`, PowerShell.

Spark code layout (`spark/app`, on `PYTHONPATH`):

- `common/` shared package: `config` (session/env), `tables` (single table registry, layer → table → path), `transforms`, `writers`, `catalog`, `metrics`, `quality` (DQ rules).
- `jobs/bronze/`, `jobs/silver/{olist,clickstream}/`, `jobs/gold/{dimensions,facts}/`: one spark-submit entry point per table, file name = table name. Each layer has `register_tables.py`.
- `jobs/ops/`: `data_quality.py --layer …`, `vacuum_tables.py`, `register_monitoring.py` (exposes `pipeline_metrics` + `data_quality_log` as `delta.monitoring.*` for the Grafana *Pipeline Monitoring* dashboard/alerts). `scripts/`: ad-hoc, not run by Airflow.
- Silver jobs write with `upsert_with_metrics(..., SILVER_KEYS[table])` (MERGE + Change Data Feed); gold still uses `write_with_metrics`. New silver table → also add its key to `SILVER_KEYS`.
- New table → add the job, add it to `common/tables.py` and the rules in `common/quality.py`, add the task in the DAG (paths via `SILVER_JOBS`/`GOLD_JOBS` in `spark_submit_defaults.py`).
- Import shared code as `from common.x import y`. No Python UDFs today, so executors never import project code; if you add one, ship `common/` with `--py-files`.

Verify:

```bash
docker compose config -q
ruff check api/ airflow/dags/ spark/app/ scripts/
python scripts/check_spark_layout.py   # imports, DAG job paths, table registry vs DQ rules
find spark/app -name '*.py' -exec python -m py_compile {} +
pip install -r tests/requirements.txt && python -m pytest tests   # needs JDK 17+; Spark tests skip without Java
```

Tests (`tests/`): registry/layout (no Spark), `common/` units, DQ rules, DAG structure (skipped without Airflow) and an end-to-end run (`test_pipeline_e2e.py`: tiny Debezium bronze fixtures in `bronze_fixtures.py` → every silver/gold job → DQ gates) on local Spark + Delta. `BRONZE_BUCKET`/`SILVER_BUCKET`/`GOLD_BUCKET` env vars override the `s3a://` buckets; conftest points them at a temp dir. New table → add a fixture to `bronze_fixtures.py` (the e2e test fails if a bronze table has none).
