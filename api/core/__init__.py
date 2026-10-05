"""
Core package for optimized FastAPI with Trino integration
"""

__version__ = "2.0.0"
__author__ = "FastAPI Trino Team"

from .database import execute_query, get_trino_config, get_trino_connection, execute_raw_query
from .sql_query import load_sql_file, build_dynamic_filters, execute_sql_with_filters, get_sql_schema

__all__ = [
    "execute_query",
    "get_trino_config",
    "get_trino_connection",
    "execute_raw_query",
    "load_sql_file",
    "build_dynamic_filters",
    "execute_sql_with_filters",
    "get_sql_schema",
]
