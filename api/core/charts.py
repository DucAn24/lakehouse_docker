"""Turn a query result into JSON-safe rows and a chart spec for the UI.

A chart spec is self-contained (labels + datasets), so the frontend only has to hand it to Chart.js:
  {"type": "bar|stacked_bar|line|pie", "x": col, "labels": [...], "datasets": [{"label": str, "data": [...]}]}
  {"type": "metric", "label": col, "value": number}
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

MAX_ROWS_TO_UI = 200
MAX_BAR_CATEGORIES = 30
MAX_LABELS = 200
MAX_SERIES = 8
MAX_PIE_SLICES = 12

CHART_TYPES = ("bar", "stacked_bar", "line", "pie")

_TIME_HINTS = ("date", "day", "week", "month", "quarter", "year", "time")
_KEY_SUFFIXES = ("_sk", "_key", "_id")


def json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _numeric_columns(columns: List[str], rows: List[Dict[str, Any]]) -> List[str]:
    numeric = []
    for col in columns:
        values = [r[col] for r in rows if r.get(col) is not None]
        if values and all(_is_number(v) for v in values):
            numeric.append(col)
    return numeric


def _is_time(column: str) -> bool:
    name = column.lower()
    return name.startswith(_TIME_HINTS) or name.endswith(_TIME_HINTS)


def _build(kind: str, x: str, ys: List[str], rows: List[Dict[str, Any]], series: Optional[str] = None) -> Optional[dict]:
    """Chart spec for x / y columns; with `series`, long-format rows (x, series, y) are pivoted."""
    if series:
        labels = list(dict.fromkeys(str(r[x]) for r in rows))
        names = list(dict.fromkeys(str(r[series]) for r in rows))
        if len(names) > MAX_SERIES:
            return None
        values = {(str(r[x]), str(r[series])): r[ys[0]] for r in rows}
        datasets = [{"label": n, "data": [values.get((lbl, n)) for lbl in labels]} for n in names]
    else:
        labels = [str(r[x]) for r in rows]
        datasets = [{"label": y, "data": [r[y] for r in rows]} for y in ys]
    if len(labels) > MAX_LABELS:
        return None
    return {"type": kind, "x": x, "labels": labels, "datasets": datasets}


def _suggest(columns: List[str], rows: List[Dict[str, Any]]) -> Optional[dict]:
    """Automatic pick when the model did not ask for a chart: metric, bar or line."""
    if not rows or not columns:
        return None

    numeric = _numeric_columns(columns, rows)
    measures = [c for c in numeric if not c.lower().endswith(_KEY_SUFFIXES) and not c.lower().startswith(_TIME_HINTS)]
    if not measures:
        return None

    if len(rows) == 1:
        if len(columns) <= 2:
            return {"type": "metric", "label": measures[0], "value": rows[0][measures[0]]}
        return None

    dimensions = [c for c in columns if c not in measures]
    if not dimensions:
        return None
    x, y = dimensions[0], measures[0]

    if _is_time(x):
        return _build("line", x, [y], rows)
    if len(rows) <= MAX_BAR_CATEGORIES:
        return _build("bar", x, [y], rows)
    return None


def _from_request(columns: List[str], rows: List[Dict[str, Any]], req: Dict[str, Any]) -> Optional[dict]:
    """The chart the model asked for, or None when the request doesn't fit the data."""
    kind = req.get("chart_type")
    if kind not in CHART_TYPES or not rows:
        return None
    numeric = set(_numeric_columns(columns, rows))
    x, series = req.get("x"), req.get("series")
    ys = [y for y in (req.get("y") or []) if y in numeric]

    if not ys or len(ys) > MAX_SERIES:
        return None
    if x not in columns or (series is not None and series not in columns):
        return None
    if series and len(ys) != 1:
        return None
    if kind == "pie":
        if len(ys) != 1 or series or len(rows) > MAX_PIE_SLICES or any((r[ys[0]] or 0) < 0 for r in rows):
            return None
    return _build(kind, x, ys, rows, series)


def resolve_chart(columns: List[str], rows: List[Dict[str, Any]], request: Optional[Dict[str, Any]] = None) -> Optional[dict]:
    """Honour the model's chart request when it fits the data; otherwise pick automatically.

    chart_type "table" means "no chart, the table is enough".
    """
    if request and request.get("chart_type") == "table":
        return None
    if request and request.get("chart_type"):
        chart = _from_request(columns, rows, request)
        if chart:
            return chart
    return _suggest(columns, rows)


def to_ui_result(
    sql: str,
    columns: List[str],
    data: List[Dict[str, Any]],
    execution_ms: Optional[int] = None,
    chart_request: Optional[Dict[str, Any]] = None,
) -> dict:
    """Structured result sent to the frontend next to the model's text answer."""
    safe = [{col: json_safe(row.get(col)) for col in columns} for row in data]
    return {
        "sql": sql,
        "columns": columns,
        "rows": safe[:MAX_ROWS_TO_UI],
        "row_count": len(safe),
        "truncated": len(safe) > MAX_ROWS_TO_UI,
        "execution_ms": execution_ms,
        "chart": resolve_chart(columns, safe, chart_request),
    }
