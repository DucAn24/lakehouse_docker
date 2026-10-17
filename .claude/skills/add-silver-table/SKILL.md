---
name: add-silver-table
description: Checklist for adding a new silver table (olist or clickstream) to the Spark pipeline.
---

Checklist (all required):
1. Job: `spark/app/jobs/silver/<olist|clickstream>/<table>.py` (file name = table name). Copy a sibling; write with `upsert_with_metrics(..., SILVER_KEYS[table])`.
2. Register the table in `spark/app/common/tables.py` and its key in `SILVER_KEYS`.
3. DQ rules in `spark/app/common/quality.py`.
4. Add the task in the Airflow DAG (path via `SILVER_JOBS` in `spark_submit_defaults.py`).
5. Ensure the layer's `register_tables.py` picks it up (UC `s3://` paths only, never `s3a://`).
6. Bronze fixture in `tests/bronze_fixtures.py` (e2e fails without one).
7. Add the source to `dbt/models/sources.yml` if gold will read it.
8. Run the `verify` skill.
