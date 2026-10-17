---
name: add-silver-table
description: Checklist for adding a new silver table (olist or clickstream) to the Spark pipeline.
---

Checklist (all required):
1. Job: `spark/app/jobs/silver/<olist|clickstream>/<table>.py` (file name = table name). Copy a sibling: read bronze with `spark.table(table_id("bronze", ...))`, write with `upsert_with_metrics(..., table_id("silver", table), SILVER_KEYS[table])`.
2. Add the table to `spark/app/common/tables.py` (identifier `lakehouse.silver.<table>`) and its key to `SILVER_KEYS`.
3. DQ rules in `spark/app/common/quality.py`.
4. Add the task in the Airflow DAG (path via `SILVER_JOBS` in `spark_submit_defaults.py`).
5. Bronze fixture in `tests/bronze_fixtures.py` (e2e fails without one).
6. Add the source to `dbt/models/sources.yml` if gold will read it.
7. Run the `verify` skill.

No catalog registration step: the table exists in Polaris (and so in Trino) once the job writes it.
