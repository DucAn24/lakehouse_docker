import re
import trino
from trino.exceptions import TrinoUserError
from typing import Dict, Any
from contextlib import contextmanager
import os
from datetime import datetime, date
import logging
from dotenv import load_dotenv

# Keywords that must never reach Trino from chatbot-generated queries
_BLOCKED_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|MERGE|REPLACE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)
_READ_QUERY_START = re.compile(r"^(SELECT|WITH)\b", re.IGNORECASE)
_MAX_QUERY_LIMIT = 1000


class QueryValidationError(ValueError):
    """Trino rejected a query at analysis time (syntax, unknown table or column, type mismatch)."""

    def __init__(self, message: str, error_name: str = ""):
        super().__init__(message)
        self.error_name = error_name


def is_safe_query(sql: str) -> tuple[bool, str]:
    """Return (True, '') if sql is safe to run, else (False, reason)."""
    if not isinstance(sql, str) or not sql.strip():
        return False, "A SQL query is required."

    normalized_sql = sql.strip()
    if "--" in normalized_sql or "/*" in normalized_sql or "*/" in normalized_sql:
        return False, "SQL comments are not allowed."

    statement = normalized_sql.rstrip(";").strip()
    if ";" in statement:
        return False, "Multiple SQL statements are not allowed."

    if not _READ_QUERY_START.match(statement):
        return False, "Only SELECT queries are permitted."

    match = _BLOCKED_KEYWORDS.search(sql)
    if match:
        return False, f"Operation '{match.group().upper()}' is not allowed. Only SELECT queries are permitted."
    return True, ""


load_dotenv()

logger = logging.getLogger(__name__)


def get_trino_config():
    return {
        "host": os.getenv("TRINO_HOST", "localhost"),
        "port": int(os.getenv("TRINO_PORT", "8080")),
        "user": os.getenv("TRINO_USER", "admin"),
        "catalog": os.getenv("TRINO_CATALOG", "lakehouse"),
        "schema": os.getenv("TRINO_SCHEMA", "gold"),
        "http_scheme": os.getenv("TRINO_HTTP_SCHEME", "http"),
        "auth": None,
    }


@contextmanager
def get_trino_connection():
    conn = None
    try:
        config = get_trino_config()
        conn = trino.dbapi.connect(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            catalog=config["catalog"],
            schema=config["schema"],
        )
        yield conn
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()


def execute_query(query: str) -> Dict[str, Any]:
    with get_trino_connection() as conn:
        cursor = conn.cursor()

        try:
            cursor.execute(query)

            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []

            rows = cursor.fetchall()

            # Format data
            data = []
            for row in rows:
                row_dict = {}
                for i, value in enumerate(row):
                    if isinstance(value, (date, datetime)):
                        row_dict[columns[i]] = value.isoformat()
                    else:
                        row_dict[columns[i]] = value
                data.append(row_dict)

            return {"count": len(data), "query_executed": query.strip(), "data": data, "columns": columns}

        finally:
            cursor.close()


def validate_query(sql: str) -> None:
    """Analyse a query in Trino without running it (EXPLAIN TYPE VALIDATE).

    Raises QueryValidationError with Trino's message, so the caller can fix the query cheaply.
    """
    try:
        execute_query(f"EXPLAIN (TYPE VALIDATE) {sql}")
    except TrinoUserError as e:
        raise QueryValidationError(e.message, e.error_name) from e


def execute_raw_query(sql: str, limit: int = 50, validate: bool = True) -> Dict[str, Any]:
    """Run a chatbot-generated SELECT query with safety checks.

    Raises ValueError if the query contains blocked operations, QueryValidationError (a ValueError)
    if Trino can't analyse it. Injects LIMIT automatically if not present.
    """
    safe, reason = is_safe_query(sql)
    if not safe:
        raise ValueError(reason)

    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise ValueError("Query limit must be a positive integer.")
    limit = min(limit, _MAX_QUERY_LIMIT)
    sql = sql.strip().rstrip(";")
    if "LIMIT" not in sql.upper():
        sql += f" LIMIT {limit}"

    if validate:
        validate_query(sql)
    return execute_query(sql)
