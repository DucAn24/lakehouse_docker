"""Metadata aggregator — merges Trino, the Iceberg catalog (Polaris), dbt YAML docs, and the table registry.

Produces a unified view of every lakehouse table with columns, descriptions, FK
relationships, DQ scores, row counts, storage locations, and freshness timestamps.
Results are cached in memory with a configurable TTL (default 5 min).
"""

import ast
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from core import iceberg, query, query_scalar

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DBT_MODELS_DIR = Path(os.getenv("DBT_MODELS_DIR", "/dbt/models"))
TABLES_PY_PATH = Path(os.getenv("TABLES_PY_PATH", "/common/tables.py"))
CACHE_TTL = int(os.getenv("METADATA_CACHE_TTL", "300"))
ROW_COUNT_TTL = int(os.getenv("ROW_COUNT_CACHE_TTL", "3600"))
TRINO_WORKERS = int(os.getenv("CATALOG_TRINO_WORKERS", "6"))
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "lakehouse")
BRONZE_SCHEMA = f"{TRINO_CATALOG}.bronze"
GOLD_SCHEMA = f"{TRINO_CATALOG}.gold"
SILVER_SCHEMA = f"{TRINO_CATALOG}.silver"
MONITORING_SCHEMA = f"{TRINO_CATALOG}.monitoring"

_REF = re.compile(r"ref\(\s*['\"]([^'\"]+)['\"]\s*\)")

# ---------------------------------------------------------------------------
# Table registry parser (reads common/tables.py as data, not import)
# ---------------------------------------------------------------------------


def _registry_literals() -> Dict[str, Any]:
    """Module-level literal assignments of common/tables.py (parsed with ast, never imported)."""
    if not TABLES_PY_PATH.exists():
        logger.warning("Table registry not found at %s", TABLES_PY_PATH)
        return {}
    try:
        tree = ast.parse(TABLES_PY_PATH.read_text(encoding="utf-8"))
    except SyntaxError as e:
        logger.warning("Cannot parse %s: %s", TABLES_PY_PATH, e)
        return {}
    values: Dict[str, Any] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            name, value = node.targets[0].id, node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value:
            name, value = node.target.id, node.value
        else:
            continue
        try:
            values[name] = ast.literal_eval(value)
        except ValueError:
            continue  # not a plain literal (e.g. the TABLES comprehension)
    return values


def _parse_table_registry() -> Dict[str, Dict[str, str]]:
    """Layer -> table -> Iceberg identifier from common/tables.py, without importing Spark."""
    values = _registry_literals()
    layers: Dict[str, Dict[str, str]] = {"bronze": {}, "silver": {}, "gold": {}}
    for layer, key in (("bronze", "_BRONZE_TOPICS"), ("silver", "_SILVER_TABLES"), ("gold", "_GOLD_TABLES")):
        for name in values.get(key, []):
            layers[layer][name] = iceberg.identifier(layer, name)
    if not any(layers.values()):
        logger.warning("No tables found in %s: has common/tables.py been refactored?", TABLES_PY_PATH)
    return layers


def _parse_silver_keys() -> Dict[str, List[str]]:
    """SILVER_KEYS from common/tables.py."""
    return {k: list(v) for k, v in _registry_literals().get("SILVER_KEYS", {}).items()}


# ---------------------------------------------------------------------------
# dbt YAML docs parser
# ---------------------------------------------------------------------------


class _DbtDocs:
    def __init__(self):
        self.table_desc: Dict[str, str] = {}
        self.column_desc: Dict[Tuple[str, str], str] = {}
        self.fks: Dict[Tuple[str, str], str] = {}          # (table, col) -> "dim.col"
        self.tests: Dict[Tuple[str, str], List[str]] = {}  # (table, col) -> test names


def _load_dbt_docs() -> _DbtDocs:
    docs = _DbtDocs()
    if not DBT_MODELS_DIR.is_dir():
        return docs
    for path in sorted(DBT_MODELS_DIR.rglob("*.yml")):
        try:
            content = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception as e:
            logger.warning("Skipping %s: %s", path, e)
            continue
        for model in content.get("models") or []:
            table = model.get("name")
            if not table:
                continue
            if model.get("description"):
                docs.table_desc[table] = str(model["description"]).strip()
            for col in model.get("columns") or []:
                name = col.get("name")
                if not name:
                    continue
                if col.get("description"):
                    docs.column_desc[(table, name)] = str(col["description"]).strip()
                col_tests = []
                for test in col.get("data_tests") or col.get("tests") or []:
                    if isinstance(test, str):
                        col_tests.append(test)
                    elif isinstance(test, dict):
                        test_name = next(iter(test))
                        col_tests.append(test_name)
                        if test_name == "relationships":
                            args = test["relationships"] or {}
                            args = args.get("arguments", args)
                            ref_match = _REF.search(str(args.get("to", "")))
                            if ref_match and args.get("field"):
                                docs.fks[(table, name)] = f"{ref_match.group(1)}.{args['field']}"
                if col_tests:
                    docs.tests[(table, name)] = col_tests
    return docs


# ---------------------------------------------------------------------------
# DQ scores from monitoring
# ---------------------------------------------------------------------------


def _load_dq_scores() -> Dict[Tuple[str, str], Dict[str, Any]]:
    """Result of the latest DQ run per (layer, table) from lakehouse.monitoring.data_quality_log."""
    scores: Dict[Tuple[str, str], Dict[str, Any]] = {}
    try:
        result = query(f"""
            SELECT l.layer, l.table_name, l.passed, l.checked_at
            FROM {MONITORING_SCHEMA}.data_quality_log l
            JOIN (
                SELECT layer, table_name, max(checked_at) AS last_run
                FROM {MONITORING_SCHEMA}.data_quality_log
                GROUP BY layer, table_name
            ) m ON l.layer = m.layer AND l.table_name = m.table_name AND l.checked_at = m.last_run
        """)
        for row in result["data"]:
            entry = scores.setdefault(
                (row.get("layer", ""), row.get("table_name", "")),
                {"total": 0, "passed": 0, "last_checked": row.get("checked_at")},
            )
            entry["total"] += 1
            if row.get("passed"):
                entry["passed"] += 1
    except Exception as e:
        logger.warning("Could not load DQ scores: %s", e)
    return scores


# ---------------------------------------------------------------------------
# Column metadata from Trino DESCRIBE
# ---------------------------------------------------------------------------


def _describe_table(schema: str, table: str) -> List[Dict[str, str]]:
    try:
        result = query(f"DESCRIBE {schema}.{table}")
        return [
            {"column": r.get("Column", r.get("column", "")),
             "type": r.get("Type", r.get("type", ""))}
            for r in result["data"]
        ]
    except Exception as e:
        logger.warning("DESCRIBE %s.%s failed: %s", schema, table, e)
        return []


def _row_count(schema: str, table: str) -> Optional[int]:
    return query_scalar(f"SELECT count(*) FROM {schema}.{table}")


# ---------------------------------------------------------------------------
# MetadataStore — cached aggregation
# ---------------------------------------------------------------------------


class MetadataStore:
    """Thread-safe, TTL-cached store of all table metadata.

    The first build is synchronous; later expiries are rebuilt in a background thread
    while the previous snapshot keeps being served. Row counts (a full scan each) are
    fetched separately, in the background, with a longer TTL.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._state_lock = threading.Lock()  # guards the _refreshing / _counting flags
        self._tables: Dict[str, List[Dict[str, Any]]] = {}   # layer -> [table_info]
        self._table_index: Dict[str, Dict[str, Any]] = {}    # "layer/table" -> info
        self._stats: Dict[str, Any] = {}
        self._expires = 0.0
        self._refreshing = False
        self._row_counts: Dict[str, Optional[int]] = {}      # "layer/table" -> count
        self._counts_expires = 0.0
        self._counting = False

    def _refresh(self) -> None:
        if time.monotonic() < self._expires:
            return
        if self._table_index:  # stale snapshot available: rebuild in the background
            with self._state_lock:
                if self._refreshing:
                    return
                self._refreshing = True
            threading.Thread(target=self._rebuild_background, daemon=True).start()
            return
        with self._lock:  # cold start: callers wait for the first build
            if time.monotonic() < self._expires:
                return
            self._rebuild()

    def _rebuild_background(self) -> None:
        try:
            with self._lock:
                self._rebuild()
        finally:
            self._refreshing = False

    def _rebuild(self) -> None:
        try:
            self._build()
            self._expires = time.monotonic() + CACHE_TTL
        except Exception as e:
            logger.error("Metadata refresh failed: %s", e)
            self._expires = time.monotonic() + 30  # retry sooner
        self._schedule_row_counts()

    def _schedule_row_counts(self) -> None:
        with self._state_lock:
            if self._counting or time.monotonic() < self._counts_expires:
                return
            self._counting = True
        threading.Thread(target=self._load_row_counts, daemon=True).start()

    def _load_row_counts(self) -> None:
        try:
            gold = [(f"gold/{t['name']}", t["name"]) for t in self._tables.get("gold", [])]
            with ThreadPoolExecutor(max_workers=TRINO_WORKERS) as pool:
                counts = list(pool.map(lambda kv: _row_count(GOLD_SCHEMA, kv[1]), gold))
            self._row_counts = {key: n for (key, _), n in zip(gold, counts)}
            self._counts_expires = time.monotonic() + ROW_COUNT_TTL
            for key, n in self._row_counts.items():
                if key in self._table_index:
                    self._table_index[key]["row_count"] = n
            self._stats["total_gold_rows"] = sum(n for n in self._row_counts.values() if n)
        except Exception as e:
            logger.warning("Row count refresh failed: %s", e)
            self._counts_expires = time.monotonic() + 60
        finally:
            self._counting = False

    def _build(self) -> None:
        registry = _parse_table_registry()
        silver_keys = _parse_silver_keys()
        docs = _load_dbt_docs()
        dq = _load_dq_scores()
        schemas = {"bronze": BRONZE_SCHEMA, "silver": SILVER_SCHEMA, "gold": GOLD_SCHEMA}

        jobs = [(layer, name) for layer in schemas for name in sorted(registry.get(layer, {}))]
        with ThreadPoolExecutor(max_workers=TRINO_WORKERS) as pool:
            described = list(pool.map(lambda j: _describe_table(schemas[j[0]], j[1]), jobs))
            locations = list(pool.map(lambda j: iceberg.location(*j), jobs))
        columns_by_table = dict(zip(jobs, described))
        location_by_table = dict(zip(jobs, locations))

        tables_by_layer: Dict[str, List[Dict[str, Any]]] = {}
        index: Dict[str, Dict[str, Any]] = {}

        for layer in schemas:
            layer_tables: List[Dict[str, Any]] = []
            for table_name, table_id in sorted(registry.get(layer, {}).items()):
                enriched_columns = [
                    {
                        "column": col["column"],
                        "type": col["type"],
                        "description": docs.column_desc.get((table_name, col["column"]), ""),
                        "fk": docs.fks.get((table_name, col["column"]), ""),
                        "tests": docs.tests.get((table_name, col["column"]), []),
                    }
                    for col in columns_by_table[(layer, table_name)]
                ]

                dq_info = dq.get((layer, table_name))
                dq_score = round(dq_info["passed"] / dq_info["total"] * 100) if dq_info and dq_info["total"] else None

                info: Dict[str, Any] = {
                    "name": table_name,
                    "layer": layer,
                    "identifier": table_id,
                    # real Iceberg location (s3://lakehouse/<layer>/<table>...); identifier until it exists
                    "path": location_by_table[(layer, table_name)] or table_id,
                    "description": docs.table_desc.get(table_name, ""),
                    "columns": enriched_columns,
                    "column_count": len(enriched_columns),
                    "row_count": self._row_counts.get(f"{layer}/{table_name}"),
                    "dq_score": dq_score,
                    "dq_last_checked": dq_info["last_checked"] if dq_info else None,
                    "primary_keys": silver_keys.get(table_name, []),
                }
                layer_tables.append(info)
                index[f"{layer}/{table_name}"] = info
            tables_by_layer[layer] = layer_tables

        self._stats = {
            "total_tables": len(index),
            "total_gold_rows": sum(n for n in self._row_counts.values() if n),
            "layers": {layer: len(tbls) for layer, tbls in tables_by_layer.items()},
        }
        self._tables = tables_by_layer
        self._table_index = index

    # -- public API --

    def all_tables(self, layer: Optional[str] = None) -> List[Dict[str, Any]]:
        self._refresh()
        if layer:
            return self._tables.get(layer, [])
        result = []
        for tables in self._tables.values():
            result.extend(tables)
        return result

    def get_table(self, layer: str, name: str) -> Optional[Dict[str, Any]]:
        self._refresh()
        return self._table_index.get(f"{layer}/{name}")

    def sample(self, layer: str, name: str, limit: int = 10) -> Dict[str, Any]:
        """Fetch sample rows from a table; raises on Trino errors."""
        schema = {"bronze": BRONZE_SCHEMA, "silver": SILVER_SCHEMA, "gold": GOLD_SCHEMA}.get(layer)
        if not schema:
            raise ValueError(f"Unknown layer '{layer}'")
        return query(f"SELECT * FROM {schema}.{name} LIMIT {max(1, min(limit, 50))}")

    def stats(self) -> Dict[str, Any]:
        self._refresh()
        return dict(self._stats)

    def search(self, q: str) -> List[Dict[str, Any]]:
        """Simple substring search across table names, descriptions, and column names."""
        self._refresh()
        q_lower = q.lower()
        results = []
        for info in self._table_index.values():
            score = 0
            matches = []
            if q_lower in info["name"].lower():
                score += 10
                matches.append(f"table name: {info['name']}")
            if q_lower in info.get("description", "").lower():
                score += 5
                matches.append("table description")
            for col in info.get("columns", []):
                if q_lower in col["column"].lower():
                    score += 3
                    matches.append(f"column: {col['column']}")
                if q_lower in col.get("description", "").lower():
                    score += 2
                    matches.append(f"column description: {col['column']}")
            if score > 0:
                results.append({
                    **{k: v for k, v in info.items() if k != "columns"},
                    "search_score": score,
                    "matches": matches[:5],
                })
        results.sort(key=lambda r: r["search_score"], reverse=True)
        return results

    def invalidate(self) -> None:
        self._expires = 0.0


store = MetadataStore()
