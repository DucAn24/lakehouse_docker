"""Catalog API — unified metadata, lineage, and search for the lakehouse portal."""

import logging
import os

from fastapi import FastAPI, Header, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from core import iceberg
from core.metadata import store
from core.lineage import lineage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Lakehouse Catalog API",
    description="Metadata catalog, lineage, and search for the lakehouse portal",
    version="1.0.0",
)

# The portal reaches this API through its nginx proxy (same origin); CORS only matters for
# direct access, so allow just the configured origins (comma-separated), no credentials.
CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "http://localhost:3002").split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Tables / Catalog
# ---------------------------------------------------------------------------

@app.get("/tables")
def list_tables(layer: str = Query(None, description="Filter by layer: bronze, silver, gold")):
    """List all tables, optionally filtered by layer."""
    tables = store.all_tables(layer=layer)
    # Return lightweight summaries (no full column list)
    return {
        "tables": [
            {
                "name": t["name"],
                "layer": t["layer"],
                "description": t["description"],
                "column_count": t["column_count"],
                "row_count": t["row_count"],
                "dq_score": t["dq_score"],
                "path": t["path"],
            }
            for t in tables
        ],
        "count": len(tables),
    }


@app.get("/tables/{layer}/{table}")
def get_table(layer: str, table: str):
    """Full table detail: columns, descriptions, FK relationships, DQ, and keys."""
    info = store.get_table(layer, table)
    if not info:
        raise HTTPException(status_code=404, detail=f"Table {layer}/{table} not found")
    return info


@app.get("/tables/{layer}/{table}/sample")
def get_sample(layer: str, table: str, limit: int = Query(10, ge=1, le=50)):
    """Fetch sample rows from a silver or gold table."""
    info = store.get_table(layer, table)
    if not info:
        raise HTTPException(status_code=404, detail=f"Table {layer}/{table} not found")
    try:
        return store.sample(layer, table, limit)
    except Exception as e:
        logger.warning("Sample query for %s/%s failed: %s", layer, table, e)
        raise HTTPException(status_code=502, detail=f"Query failed: {e}")


# ---------------------------------------------------------------------------
# Iceberg metadata (PyIceberg → Polaris): snapshots, snapshot log, format properties
# ---------------------------------------------------------------------------

def _iceberg(fetch, layer: str, table: str, *args):
    if not store.get_table(layer, table):
        raise HTTPException(status_code=404, detail=f"Table {layer}/{table} not found")
    try:
        return fetch(layer, table, *args)
    except iceberg.NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table {layer}/{table} has not been written yet")
    except Exception as e:
        logger.warning("Iceberg metadata for %s/%s failed: %s", layer, table, e)
        raise HTTPException(status_code=502, detail=f"Iceberg catalog request failed: {e}")


@app.get("/tables/{layer}/{table}/snapshots")
def get_snapshots(layer: str, table: str, limit: int = Query(50, ge=1, le=500)):
    """Iceberg snapshots, newest first: operation and added / deleted / total record counts."""
    snaps = _iceberg(iceberg.snapshots, layer, table, limit)
    return {"table": iceberg.identifier(layer, table), "snapshots": snaps, "count": len(snaps)}


@app.get("/tables/{layer}/{table}/history")
def get_history(layer: str, table: str):
    """Iceberg snapshot log: which snapshot was current when (time-travel targets)."""
    return {"table": iceberg.identifier(layer, table), "history": _iceberg(iceberg.history, layer, table)}


@app.get("/tables/{layer}/{table}/properties")
def get_properties(layer: str, table: str):
    """Iceberg format metadata: location, current snapshot, partition spec, sort order, table properties."""
    return _iceberg(iceberg.table_properties, layer, table)


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

@app.get("/search")
def search_tables(q: str = Query(..., min_length=1, description="Search query")):
    """Search tables, columns, and descriptions."""
    return {"results": store.search(q), "query": q}


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------

@app.get("/lineage")
def get_lineage():
    """Full lineage graph (all layers)."""
    return lineage.full_graph()


@app.get("/lineage/{table}")
def get_table_lineage(table: str):
    """Upstream/downstream lineage for one table."""
    result = lineage.table_lineage(table)
    if not result["nodes"]:
        raise HTTPException(status_code=404, detail=f"No lineage found for table '{table}'")
    return result


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@app.get("/stats")
def get_stats():
    """Platform-level statistics."""
    return store.stats()


# ---------------------------------------------------------------------------
# Cache management
# ---------------------------------------------------------------------------

@app.post("/cache/invalidate")
def invalidate_cache(x_requested_with: str = Header(None)):
    """Force refresh metadata and lineage caches.

    Requires a custom header: browsers then send a CORS preflight, which foreign origins fail,
    so a random web page cannot trigger rebuilds with a cross-site POST.
    """
    if x_requested_with != "catalog-portal":
        raise HTTPException(status_code=403, detail="Missing X-Requested-With header")
    store.invalidate()
    lineage.invalidate()
    iceberg.invalidate()
    return {"message": "Cache invalidated"}
