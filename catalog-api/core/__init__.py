"""Trino query helper for the catalog API.

Thin wrapper matching the pattern in api/core/database.py — read-only queries only.
"""

import logging
import os
from contextlib import contextmanager
from datetime import date, datetime
from typing import Any, Dict, List

import trino

logger = logging.getLogger(__name__)


def _config() -> dict:
    return {
        "host": os.getenv("TRINO_HOST", "localhost"),
        "port": int(os.getenv("TRINO_PORT", "8080")),
        "user": os.getenv("TRINO_USER", "admin"),
        "catalog": os.getenv("TRINO_CATALOG", "lakehouse"),
        "schema": os.getenv("TRINO_SCHEMA", "gold"),
    }


@contextmanager
def _connection():
    cfg = _config()
    conn = trino.dbapi.connect(
        host=cfg["host"], port=cfg["port"], user=cfg["user"],
        catalog=cfg["catalog"], schema=cfg["schema"],
    )
    try:
        yield conn
    finally:
        conn.close()


def query(sql: str) -> Dict[str, Any]:
    """Execute a read-only SQL statement and return {columns, data, count}."""
    with _connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            columns = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
            data: List[Dict[str, Any]] = []
            for row in rows:
                record = {}
                for i, val in enumerate(row):
                    if isinstance(val, (date, datetime)):
                        record[columns[i]] = val.isoformat()
                    else:
                        record[columns[i]] = val
                data.append(record)
            return {"columns": columns, "data": data, "count": len(data)}
        finally:
            cur.close()


def query_scalar(sql: str) -> Any:
    """Return a single scalar value, or None on failure."""
    try:
        result = query(sql)
        if result["data"]:
            return next(iter(result["data"][0].values()))
    except Exception as e:
        logger.warning("Scalar query failed (%s): %s", sql[:80], e)
    return None
