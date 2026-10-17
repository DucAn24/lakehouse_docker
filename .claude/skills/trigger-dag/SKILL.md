---
name: trigger-dag
description: List, trigger, and inspect Airflow DAG runs in the airflow container.
---

```powershell
docker compose exec airflow airflow dags list
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow dags trigger <dag_id>
docker compose exec airflow airflow dags list-runs -d <dag_id>
docker compose exec airflow airflow tasks states-for-dag-run <dag_id> <run_id>
docker compose exec airflow airflow tasks test <dag_id> <task_id> 2026-01-01
```

Pipeline order: bronze → silver (spark-submit) → `dbt_build` → `dq_gold` → `maintain_iceberg_tables`. For a failed task, read its log: `docker compose logs airflow --tail=200`, and for Spark jobs check spark-master. Offline DAG checks: `python scripts/check_dags.py`.
