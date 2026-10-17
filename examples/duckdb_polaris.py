"""Query the lakehouse with DuckDB: attach the Polaris catalog and use plain SQL.

    pip install -r examples/requirements.txt
    python examples/duckdb_polaris.py

DuckDB's iceberg extension talks to Polaris (the Iceberg REST catalog `lakehouse`) and reads the
Parquet files from MinIO directly, on the host ports (localhost:8181 / localhost:9000). Polaris vends
no credentials here, so the MinIO keys come from an S3 secret (ACCESS_DELEGATION_MODE 'none').
"""

from __future__ import annotations

from urllib.parse import urlparse

import duckdb
from _env import env

QUERY = """
    SELECT c.customer_state, count(*) AS order_items, round(sum(f.total_item_value), 2) AS revenue
    FROM lakehouse.gold.fact_orders f
    JOIN lakehouse.gold.dim_customer c ON f.customer_sk = c.customer_sk
    GROUP BY c.customer_state
    ORDER BY revenue DESC
    LIMIT 10
"""


def connect() -> duckdb.DuckDBPyConnection:
    polaris = env("POLARIS_URI", "http://localhost:8181/api/catalog")
    s3 = urlparse(env("S3_HOST_ENDPOINT", "http://localhost:9000"))
    con = duckdb.connect()
    for ext in ("httpfs", "iceberg"):
        con.install_extension(ext)
        con.load_extension(ext)
    con.execute(
        f"""
        CREATE SECRET minio (
            TYPE s3, KEY_ID '{env('MINIO_ROOT_USER', 'admin')}', SECRET '{env('MINIO_ROOT_PASSWORD', 'password123')}',
            ENDPOINT '{s3.netloc}', URL_STYLE 'path', USE_SSL {str(s3.scheme == 'https').lower()},
            REGION '{env('AWS_REGION', 'us-east-1')}'
        )
        """
    )
    con.execute(
        f"""
        CREATE SECRET polaris (
            TYPE iceberg,
            CLIENT_ID '{env('POLARIS_ROOT_CLIENT_ID', 'root')}', CLIENT_SECRET '{env('POLARIS_ROOT_CLIENT_SECRET', 's3cr3t')}',
            OAUTH2_SERVER_URI '{polaris}/v1/oauth/tokens', OAUTH2_SCOPE 'PRINCIPAL_ROLE:ALL'
        )
        """
    )
    con.execute(
        f"ATTACH 'lakehouse' AS lakehouse (TYPE iceberg, ENDPOINT '{polaris}', SECRET polaris, "
        "ACCESS_DELEGATION_MODE 'none')"
    )
    return con


def main() -> None:
    con = connect()
    # printed via pandas: DuckDB's box-drawing table output breaks on non-UTF-8 consoles (Windows)
    print("Tables in the lakehouse catalog:")
    tables = "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'lakehouse' ORDER BY 1, 2"
    print(con.sql(tables).df().to_string(index=False))
    print("\nRevenue by customer state (gold star schema):")
    print(con.sql(QUERY).df().to_string(index=False))


if __name__ == "__main__":
    main()
