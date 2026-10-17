---
name: add-gold-model
description: Checklist for adding a new gold dbt model (dimension or fact) on Trino.
---

1. Model: `dbt/models/{dimensions,facts}/<table>.sql`; file name = gold table name.
2. First line: `{{ config(properties=gold_properties()) }}`.
3. Surrogate keys via `hash_key([...])`; dimensions get an UNKNOWN member `-1`; facts use `coalesce(fk, -1)`; dates via `date_key(ts)`.
4. Sources are `delta.silver.*` from `models/sources.yml`.
5. Tests + docs in `_dimensions.yml` / `_facts.yml` (include `unknown_member_ratio` on FKs, plus descriptions/FKs; the chatbot schema context reads them).
6. Add to `spark/app/common/tables.py` and rules in `get_gold_checks()` in `common/quality.py`.
7. Run `python scripts/check_spark_layout.py` (compares models vs registry), then the `verify` skill.
