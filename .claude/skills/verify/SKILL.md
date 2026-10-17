---
name: verify
description: Run the project's lint, compose, layout and test checks before finishing a change. Use after editing code, DAGs, dbt models, or docker-compose.yml.
---

Run from the repo root and report each result faithfully:

```bash
docker compose config -q
ruff check api/ airflow/dags/ spark/app/ scripts/ catalog-api/
python scripts/check_spark_layout.py
python scripts/check_dags.py
find spark/app -name '*.py' -exec python -m py_compile {} +
python -m pytest tests -x -q
```

Notes:
- pytest e2e/gold tests skip without JDK 17+ / Docker; say so if skipped.
- For quick iterations run only the relevant file, e.g. `python -m pytest tests/test_quality.py -q`.
- Fix `ruff` issues with `ruff check --fix` only for the files you touched.
