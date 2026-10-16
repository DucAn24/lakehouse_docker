"""Lineage graph builder — derives the full data flow from dbt model SQL + table registry.

Nodes represent tables at each layer; edges represent data flow (source → derived).
The dbt models' ref()/source() calls provide gold ← silver edges; the table registry provides
bronze → silver mapping (same table names).  Source (Postgres) → bronze edges come
from the _BRONZE_TOPICS naming convention (olist.public.X / clickstream.public.X).
"""

import logging
import os
import re
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

from core.metadata import _parse_table_registry

logger = logging.getLogger(__name__)

DBT_MODELS_DIR = Path(os.getenv("DBT_MODELS_DIR", "/dbt/models"))
CACHE_TTL = int(os.getenv("LINEAGE_CACHE_TTL", "300"))

# Maps source database names in the Debezium topic to a friendly name
_SOURCE_DATABASES = {
    "olist": "Postgres (Olist)",
    "clickstream": "Postgres (Clickstream)",
}


_REF_RE = re.compile(r"""\{\{\s*ref\(\s*['"]([^'"]+)['"]\s*\)""")
_SOURCE_RE = re.compile(r"""\{\{\s*source\(\s*['"][^'"]+['"]\s*,\s*['"]([^'"]+)['"]\s*\)""")


def _parse_dbt_models() -> Dict[str, List[str]]:
    """Scan dbt model SQL for ref()/source() → {child_table: [parent_tables]}.

    Reads the mounted model files rather than target/manifest.json: Airflow writes
    the manifest to its own /tmp, so it never reaches this container.
    """
    parent_map: Dict[str, List[str]] = {}
    if not DBT_MODELS_DIR.is_dir():
        logger.info("dbt models dir %s not found — lineage will use registry only", DBT_MODELS_DIR)
        return parent_map
    for sql_file in DBT_MODELS_DIR.rglob("*.sql"):
        try:
            text = sql_file.read_text(encoding="utf-8")
        except OSError as e:
            logger.warning("Failed to read %s: %s", sql_file, e)
            continue
        parents = list(dict.fromkeys(_REF_RE.findall(text) + _SOURCE_RE.findall(text)))
        if parents:
            parent_map[sql_file.stem] = parents
    return parent_map


class LineageGraph:
    """Thread-safe cached lineage graph."""

    def __init__(self):
        self._lock = threading.Lock()
        self._nodes: List[Dict[str, Any]] = []
        self._edges: List[Dict[str, str]] = []
        self._expires = 0.0

    def _refresh(self) -> None:
        if time.monotonic() < self._expires:
            return
        with self._lock:
            if time.monotonic() < self._expires:
                return
            try:
                self._build()
                self._expires = time.monotonic() + CACHE_TTL
            except Exception as e:
                logger.error("Lineage build failed: %s", e)
                self._expires = time.monotonic() + 30

    def _build(self) -> None:
        registry = _parse_table_registry()
        dbt_parents = _parse_dbt_models()

        nodes: Dict[str, Dict[str, Any]] = {}
        edges: List[Dict[str, str]] = []
        seen_edges: Set[Tuple[str, str]] = set()

        def add_node(node_id: str, label: str, layer: str, table: str = "") -> None:
            if node_id not in nodes:
                nodes[node_id] = {
                    "id": node_id,
                    "label": label,
                    "layer": layer,
                    "table": table,
                }

        def add_edge(source: str, target: str) -> None:
            key = (source, target)
            if key not in seen_edges:
                seen_edges.add(key)
                edges.append({"source": source, "target": target})

        # 1. Source nodes (Postgres databases)
        sources_seen: Set[str] = set()
        for table_name, topic in registry.get("bronze", {}).items():
            # topic format: "olist.public.olist_orders"
            parts = topic.rstrip("/").split("/")[-1].split(".")
            if len(parts) >= 2:
                db_name = parts[0]
                if db_name not in sources_seen:
                    sources_seen.add(db_name)
                    add_node(f"source/{db_name}", _SOURCE_DATABASES.get(db_name, db_name), "source")

        # 2. Bronze nodes + source→bronze edges
        for table_name, path in registry.get("bronze", {}).items():
            node_id = f"bronze/{table_name}"
            add_node(node_id, table_name, "bronze", table_name)
            # Derive source from topic
            topic_parts = path.split("/")
            for part in topic_parts:
                if "." in part:
                    db_name = part.split(".")[0]
                    if db_name in sources_seen:
                        add_edge(f"source/{db_name}", node_id)
                    break

        # 3. Silver nodes + bronze→silver edges
        for table_name in registry.get("silver", {}):
            node_id = f"silver/{table_name}"
            add_node(node_id, table_name, "silver", table_name)
            # Same-name bronze table feeds the silver table
            bronze_id = f"bronze/{table_name}"
            if bronze_id in nodes:
                add_edge(bronze_id, node_id)

        # 4. Gold nodes + silver→gold edges (from dbt models)
        for table_name in registry.get("gold", {}):
            node_id = f"gold/{table_name}"
            add_node(node_id, table_name, "gold", table_name)

            # dbt parents
            for parent in dbt_parents.get(table_name, []):
                # Parents could be other gold models or silver sources
                parent_gold = f"gold/{parent}"
                parent_silver = f"silver/{parent}"
                if parent_gold in nodes:
                    add_edge(parent_gold, node_id)
                elif parent_silver in nodes:
                    add_edge(parent_silver, node_id)
                else:
                    # Unknown parent — create as silver (most likely a source)
                    add_node(parent_silver, parent, "silver", parent)
                    add_edge(parent_silver, node_id)

        self._nodes = list(nodes.values())
        self._edges = edges

    def full_graph(self) -> Dict[str, Any]:
        self._refresh()
        return {"nodes": self._nodes, "edges": self._edges}

    def table_lineage(self, table: str) -> Dict[str, Any]:
        """Upstream and downstream lineage for a single table."""
        self._refresh()
        # Find all node IDs containing this table name
        table_ids = {n["id"] for n in self._nodes if n.get("table") == table}
        if not table_ids:
            return {"nodes": [], "edges": [], "upstream": [], "downstream": []}

        upstream: Set[str] = set()
        downstream: Set[str] = set()

        def find_upstream(node_id: str) -> None:
            for edge in self._edges:
                if edge["target"] == node_id and edge["source"] not in upstream:
                    upstream.add(edge["source"])
                    find_upstream(edge["source"])

        def find_downstream(node_id: str) -> None:
            for edge in self._edges:
                if edge["source"] == node_id and edge["target"] not in downstream:
                    downstream.add(edge["target"])
                    find_downstream(edge["target"])

        for tid in table_ids:
            find_upstream(tid)
            find_downstream(tid)

        relevant = table_ids | upstream | downstream
        return {
            "nodes": [n for n in self._nodes if n["id"] in relevant],
            "edges": [e for e in self._edges if e["source"] in relevant and e["target"] in relevant],
            "upstream": [n["id"] for n in self._nodes if n["id"] in upstream],
            "downstream": [n["id"] for n in self._nodes if n["id"] in downstream],
            "focus": list(table_ids),
        }

    def invalidate(self) -> None:
        self._expires = 0.0


lineage = LineageGraph()
