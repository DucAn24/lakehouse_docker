"""Schema section of the chatbot system prompt, built from the live gold layer.

Columns and types come from Trino (`DESCRIBE`), so they can't drift from what dbt built. Table and
column descriptions and the foreign keys come from the dbt model YAMLs (`relationships` tests),
mounted read-only at DBT_MODELS_DIR; without them the schema is still listed, just without join hints.
"""

import logging
import os
import re
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml

from core.database import execute_query

logger = logging.getLogger(__name__)

GOLD_SCHEMA = "delta.gold"
DBT_MODELS_DIR = Path(os.getenv("DBT_MODELS_DIR", "/dbt/models"))
CACHE_TTL_SECONDS = int(os.getenv("SCHEMA_CACHE_TTL", "600"))
RETRY_AFTER_FAILURE_SECONDS = 30

_REF = re.compile(r"ref\(\s*['\"]([^'\"]+)['\"]\s*\)")
_GOLD_TABLE = re.compile(r"\bdelta\.gold\.([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE)

STATIC_NOTES = """\
CONVENTIONS
  • Two separate star schemas share only dim_date: Olist e-commerce (Brazil: dim_customer, dim_product,
    dim_seller, fact_orders, fact_reviews) and Clickstream (global: tables named *click*, plus dim_device,
    dim_traffic_source, fact_sessions, fact_clickstream_events). Never join one star's dimensions to
    the other star's facts.
  • *_sk columns are surrogate keys (hashes), only meaningful for joins. date_sk is an int yyyyMMdd.
  • Every dimension except dim_date has an UNKNOWN member with key -1; facts point to it when a lookup
    failed. Exclude -1 rows when ranking or listing dimension members.
"""


class _Docs:
    def __init__(self):
        self.table_desc: Dict[str, str] = {}
        self.column_desc: Dict[Tuple[str, str], str] = {}
        self.fks: Dict[Tuple[str, str], str] = {}  # (table, column) -> "dim_table.column"


def _load_dbt_docs() -> _Docs:
    docs = _Docs()
    if not DBT_MODELS_DIR.is_dir():
        return docs
    for path in sorted(DBT_MODELS_DIR.rglob("*.yml")):
        try:
            content = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception as e:  # a broken YAML must not take the chatbot down
            logger.warning("Skipping %s: %s", path, e)
            continue
        for model in content.get("models") or []:
            table = model.get("name")
            if not table:
                continue
            if model.get("description"):
                docs.table_desc[table] = str(model["description"]).strip()
            for column in model.get("columns") or []:
                name = column.get("name")
                if column.get("description"):
                    docs.column_desc[(table, name)] = str(column["description"]).strip()
                for test in column.get("data_tests") or column.get("tests") or []:
                    if not isinstance(test, dict) or "relationships" not in test:
                        continue
                    args = test["relationships"] or {}
                    args = args.get("arguments", args)
                    match = _REF.search(str(args.get("to", "")))
                    if match and args.get("field"):
                        docs.fks[(table, name)] = f"{match.group(1)}.{args['field']}"
    return docs


def _describe(table: str) -> List[Tuple[str, str]]:
    rows = execute_query(f"DESCRIBE {GOLD_SCHEMA}.{table}")["data"]
    return [(r.get("Column", r.get("column", "")), r.get("Type", r.get("type", ""))) for r in rows]


class SchemaContext:
    """Thread-safe cache of the gold schema, refreshed after CACHE_TTL_SECONDS."""

    def __init__(self):
        self._lock = threading.Lock()
        self._tables: Dict[str, List[Tuple[str, str]]] = {}
        self._docs = _Docs()
        self._prompt: Optional[str] = None
        self._expires = 0.0

    def _refresh(self) -> None:
        if self._prompt is not None and time.monotonic() < self._expires:
            return
        with self._lock:
            if self._prompt is not None and time.monotonic() < self._expires:
                return
            try:
                names = sorted(
                    next(iter(row.values())) for row in execute_query(f"SHOW TABLES FROM {GOLD_SCHEMA}")["data"]
                )
                self._tables = {name: _describe(name) for name in names}
                self._docs = _load_dbt_docs()
                self._prompt = self._render()
                self._expires = time.monotonic() + CACHE_TTL_SECONDS
            except Exception as e:
                logger.error("Could not load the gold schema: %s", e)
                if self._prompt is None:
                    # Nothing cached yet: tell the model to discover the schema itself, retry soon.
                    self._prompt = (
                        f"Schema of {GOLD_SCHEMA} is unavailable right now. "
                        "Call list_tables and describe_table before writing a query.\n"
                    )
                    self._expires = time.monotonic() + RETRY_AFTER_FAILURE_SECONDS
                    self._tables = {}
                else:  # keep serving the stale schema a bit longer
                    self._expires = time.monotonic() + RETRY_AFTER_FAILURE_SECONDS

    def _render(self) -> str:
        docs = self._docs
        lines = [f"GOLD LAYER — {GOLD_SCHEMA}.<table> (always use the full name)", ""]
        for table, columns in self._tables.items():
            desc = docs.table_desc.get(table)
            lines.append(f"• {table}" + (f" — {desc}" if desc else ""))
            for name, col_type in columns:
                extra = []
                if (table, name) in docs.fks:
                    extra.append(f"→ {docs.fks[(table, name)]}")
                if (table, name) in docs.column_desc:
                    extra.append(docs.column_desc[(table, name)])
                lines.append(f"    {name} {col_type}" + (f"  ({'; '.join(extra)})" if extra else ""))
            lines.append("")
        lines.append(STATIC_NOTES)
        return "\n".join(lines)

    def prompt_section(self) -> str:
        self._refresh()
        return self._prompt or ""

    def tables_used_in(self, sql: str) -> Dict[str, List[Dict[str, str]]]:
        """Columns of the delta.gold tables a query refers to; the model's hint after a failed query."""
        self._refresh()
        used = {}
        for name in dict.fromkeys(m.lower() for m in _GOLD_TABLE.findall(sql)):
            if name in self._tables:
                used[name] = [{"column": c, "type": t} for c, t in self._tables[name]]
        return used

    def invalidate(self) -> None:
        self._expires = 0.0


schema_context = SchemaContext()
