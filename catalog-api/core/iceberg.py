"""Iceberg metadata straight from the Polaris REST catalog (PyIceberg).

Trino's DESCRIBE gives columns; this adds what only the table format knows: the table location,
its snapshots (one per Spark MERGE / dbt rebuild, with added / deleted record counts), the snapshot
log, partitioning, sort order and table properties. Only table metadata is read (served by Polaris
in the load-table response), no data files. Results are cached briefly per table.
"""

import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table

logger = logging.getLogger(__name__)

CATALOG_NAME = os.getenv("ICEBERG_CATALOG", "lakehouse")
CACHE_TTL = int(os.getenv("ICEBERG_CACHE_TTL", "60"))

__all__ = ["NoSuchTableError", "identifier", "location", "table_properties", "snapshots", "history", "invalidate"]

_catalog: Optional[Catalog] = None
_catalog_lock = threading.Lock()
_cache: Dict[str, Tuple[float, Table]] = {}


def _get_catalog() -> Catalog:
    global _catalog
    with _catalog_lock:
        if _catalog is None:
            _catalog = load_catalog(
                CATALOG_NAME,
                **{
                    "type": "rest",
                    "uri": os.getenv("POLARIS_URI", "http://polaris:8181/api/catalog"),
                    "warehouse": CATALOG_NAME,
                    "credential": f"{os.getenv('POLARIS_CLIENT_ID', 'root')}:{os.getenv('POLARIS_CLIENT_SECRET', 's3cr3t')}",
                    "scope": "PRINCIPAL_ROLE:ALL",
                    # PyIceberg asks for vended credentials by default, which Polaris rejects for this
                    # catalog (stsUnavailable); an empty header means no delegation
                    "header.X-Iceberg-Access-Delegation": "",
                    # S3 with this service's own keys (Polaris vends no credentials)
                    "s3.endpoint": os.getenv("S3_ENDPOINT", "http://s3:8333"),
                    "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "admin"),
                    "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "password123"),
                    "s3.region": os.getenv("AWS_REGION", "us-east-1"),
                },
            )
        return _catalog


def identifier(layer: str, table: str) -> str:
    return f"{CATALOG_NAME}.{layer}.{table}"


def _load(layer: str, table: str) -> Table:
    key = f"{layer}.{table}"
    cached = _cache.get(key)
    if cached and time.monotonic() < cached[0]:
        return cached[1]
    loaded = _get_catalog().load_table((layer, table))
    _cache[key] = (time.monotonic() + CACHE_TTL, loaded)
    return loaded


def _iso(ms: Optional[int]) -> Optional[str]:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat() if ms is not None else None


def location(layer: str, table: str) -> Optional[str]:
    """Storage location of the table, or None when it cannot be loaded."""
    try:
        return _load(layer, table).location()
    except Exception as e:
        logger.warning("Iceberg location of %s.%s unavailable: %s", layer, table, e)
        return None


def table_properties(layer: str, table: str) -> Dict[str, Any]:
    """Format-level metadata: location, current snapshot, partition spec, sort order, properties."""
    tbl = _load(layer, table)
    meta = tbl.metadata
    schema = tbl.schema()
    current = tbl.current_snapshot()

    def column(source_id: int) -> str:
        try:
            return schema.find_field(source_id).name
        except ValueError:  # column dropped since the spec was defined
            return str(source_id)

    return {
        "identifier": identifier(layer, table),
        "location": tbl.location(),
        "metadata_location": tbl.metadata_location,
        "format_version": meta.format_version,
        "table_uuid": str(meta.table_uuid),
        "last_updated": _iso(meta.last_updated_ms),
        "current_snapshot_id": current.snapshot_id if current else None,
        "snapshot_count": len(meta.snapshots),
        "partition_spec": [
            {"column": column(f.source_id), "transform": str(f.transform), "name": f.name} for f in tbl.spec().fields
        ],
        "sort_order": [
            {"column": column(f.source_id), "transform": str(f.transform), "direction": f.direction.value}
            for f in tbl.sort_order().fields
        ],
        "properties": dict(tbl.properties),
    }


def snapshots(layer: str, table: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Snapshots newest first, with the operation and Iceberg's summary counters."""
    tbl = _load(layer, table)
    current = tbl.current_snapshot()
    result = []
    for snap in sorted(tbl.snapshots(), key=lambda s: s.timestamp_ms, reverse=True)[:limit]:
        summary = snap.summary
        counters = dict(summary.additional_properties) if summary else {}
        result.append(
            {
                "snapshot_id": snap.snapshot_id,
                "parent_id": snap.parent_snapshot_id,
                "committed_at": _iso(snap.timestamp_ms),
                "operation": summary.operation.value if summary else None,
                "is_current": bool(current and snap.snapshot_id == current.snapshot_id),
                "added_records": _int(counters.get("added-records")),
                "deleted_records": _int(counters.get("deleted-records")),
                "total_records": _int(counters.get("total-records")),
                "added_files": _int(counters.get("added-data-files")),
                "total_files": _int(counters.get("total-data-files")),
                "total_size_bytes": _int(counters.get("total-files-size")),
                "summary": counters,
            }
        )
    return result


def history(layer: str, table: str) -> List[Dict[str, Any]]:
    """Snapshot log: which snapshot was current when (oldest first), as used for time travel."""
    return [
        {"snapshot_id": entry.snapshot_id, "made_current_at": _iso(entry.timestamp_ms)}
        for entry in _load(layer, table).history()
    ]


def invalidate() -> None:
    _cache.clear()


def _int(value: Optional[str]) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None
