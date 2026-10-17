"""Read the lakehouse from plain Python with PyIceberg — no Spark, no Trino.

    pip install -r examples/requirements.txt
    python examples/pyiceberg_quickstart.py [--table gold.fact_orders]

Connects to Polaris (the Iceberg REST catalog `lakehouse`) and MinIO on their host ports
(localhost:8181 / localhost:9000), with the credentials from .env when present.
"""

from __future__ import annotations

import argparse

from _env import env
from pyiceberg.catalog import load_catalog


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--table", default="gold.fact_orders", help="<namespace>.<table>")
    parser.add_argument("--rows", type=int, default=5)
    args = parser.parse_args()

    catalog = load_catalog(
        "lakehouse",
        **{
            "type": "rest",
            "uri": env("POLARIS_URI", "http://localhost:8181/api/catalog"),
            "warehouse": "lakehouse",
            "credential": f"{env('POLARIS_ROOT_CLIENT_ID', 'root')}:{env('POLARIS_ROOT_CLIENT_SECRET', 's3cr3t')}",
            "scope": "PRINCIPAL_ROLE:ALL",
            # no credential vending (Polaris has none to give here): read MinIO with the keys below
            "header.X-Iceberg-Access-Delegation": "",
            "s3.endpoint": env("S3_HOST_ENDPOINT", "http://localhost:9000"),
            "s3.access-key-id": env("MINIO_ROOT_USER", "admin"),
            "s3.secret-access-key": env("MINIO_ROOT_PASSWORD", "password123"),
            "s3.region": env("AWS_REGION", "us-east-1"),
        },
    )

    print("Namespaces and tables:")
    for (namespace,) in catalog.list_namespaces():
        tables = sorted(name for _, name in catalog.list_tables(namespace))
        print(f"  {namespace}: {', '.join(tables) or '(empty)'}")

    table = catalog.load_table(args.table)
    print(f"\n{args.table} @ {table.location()}")
    print(table.schema())

    print("\nSnapshots (oldest first):")
    snapshots = table.snapshots()
    for snap in snapshots:
        summary = snap.summary.additional_properties if snap.summary else {}
        print(
            f"  {snap.snapshot_id}  {snap.summary.operation.value if snap.summary else '?':<9}"
            f"  +{summary.get('added-records', 0)} -{summary.get('deleted-records', 0)}"
            f"  total={summary.get('total-records', '?')}"
        )

    print(f"\nFirst {args.rows} rows (current snapshot):")
    print(table.scan(limit=args.rows).to_arrow().to_pandas().to_string(index=False))

    if len(snapshots) > 1:
        first = snapshots[0]
        rows = table.scan(snapshot_id=first.snapshot_id).count()
        print(f"\nTime travel: {rows} rows in the first snapshot ({first.snapshot_id})")


if __name__ == "__main__":
    main()
