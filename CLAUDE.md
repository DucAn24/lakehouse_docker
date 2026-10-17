@AGENTS.md

# Claude Code notes

- Host is Windows: use PowerShell / `curl.exe`; compose commands run from the repo root.
- Never commit `.env`; keep `.env.example` placeholders intact.
- Code is bind-mounted, so edits to `spark/app`, `airflow/dags`, `dbt/`, `api/`, `catalog-api/` need no rebuild. Rebuild only for Dockerfile / `requirements.txt` changes.
- Before finishing any change run the `verify` skill (or the commands under "Verify" in AGENTS.md).
- New tables touch several places (registry, DQ rules, DAG, fixtures); use the `add-silver-table` / `add-gold-model` skills.

Project skills (`.claude/skills/`): `verify`, `add-silver-table`, `add-gold-model`, `stack-ops`, `trigger-dag`.
