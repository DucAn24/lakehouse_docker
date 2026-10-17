---
name: stack-ops
description: Start, stop, rebuild, seed, and inspect the Docker Compose lakehouse stack and its services.
---

PowerShell, repo root:

- Core stack: `docker compose up -d`; with apps: `docker compose --profile apps up -d`
- Status: `docker compose ps`; logs: `docker compose logs -f --tail=100 <svc>`
- Rebuild one service (Dockerfile/requirements changes only): `docker compose build <svc>; docker compose up -d <svc>`
- Restart after config change: `docker compose up -d --force-recreate <svc>`
- Seed Postgres: `.\scripts\seed-data.ps1`; register Debezium connectors: `.\scripts\register-connectors.ps1`
- Trino shell: `docker compose exec trino trino`
- Service names: postgres, minio, kafka, connect, polaris (+ one-shot polaris-db-init, polaris-bootstrap, polaris-init), trino, spark-master, spark-worker, airflow; apps: metabase, grafana, kafka-ui, fastapi, catalog-api, portal, polaris-console.
- Polaris health: `curl.exe -s localhost:8182/q/health`; re-run catalog/namespace setup (idempotent): `docker compose up polaris-init`.

Order for a fresh start: up -d → seed-data → register-connectors → trigger the DAG. Do not run `docker compose down -v` without asking (deletes data volumes).
