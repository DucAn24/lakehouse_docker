"""Shared test setup.

Spark-backed tests run a local Spark session built by the real `common.config.create_spark_session`,
writing Iceberg tables to the `lakehouse` catalog:
  - with Docker: a throwaway Polaris + S3 (SeaweedFS) (iceberg_lake.py), the same REST catalog as the stack;
    Trino joins it on demand (fixture `trino_lake`) so dbt builds gold over the very same tables;
  - without Docker: a local file-based Iceberg catalog under a temp dir (gold tests skip).
They are skipped when Java is missing; run them with `pip install -r tests/requirements.txt` and a
JDK 17+. The Iceberg jars are fetched by Spark from Maven Central on first use.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "spark" / "app"))
sys.path.insert(0, str(ROOT / "airflow" / "dags"))

LAKE = Path(tempfile.mkdtemp(prefix="lakehouse_test_"))


def pytest_sessionfinish(session, exitstatus):
    shutil.rmtree(LAKE, ignore_errors=True)


def _java_available() -> bool:
    java_home = os.environ.get("JAVA_HOME")
    return bool(shutil.which("java") or (java_home and (Path(java_home) / "bin").exists()))


@pytest.fixture(scope="session")
def iceberg_lake():
    """S3 (SeaweedFS) + Polaris containers (see iceberg_lake.py), or None without Docker."""
    from iceberg_lake import IcebergLake, docker_available

    if not docker_available():
        yield None
        return
    lake = IcebergLake()
    try:
        lake.start()
        yield lake
    finally:
        lake.stop()


@pytest.fixture(scope="session")
def spark(request):
    if not _java_available():
        pytest.skip("Java not found (Spark tests need a JDK 17+)")
    pytest.importorskip("pyspark", reason="pyspark not installed")
    from spark_submit_defaults import SPARK_PACKAGES

    lake = request.getfixturevalue("iceberg_lake")
    if lake is not None:
        os.environ.update(lake.spark_env())
    else:
        os.environ.update(ICEBERG_CATALOG_TYPE="hadoop", ICEBERG_WAREHOUSE=(LAKE / "warehouse").as_uri())
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--master local[2] --packages {SPARK_PACKAGES} --conf spark.ui.enabled=false pyspark-shell"
    )
    from common.config import create_spark_session

    session = create_spark_session("lakehouse-tests")
    session.conf.set("spark.sql.shuffle.partitions", "2")
    session.conf.set("spark.sql.session.timeZone", "UTC")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def trino_lake(spark, iceberg_lake):
    """Trino on the test catalog, to run the dbt gold models (needs Docker and dbt-trino)."""
    pytest.importorskip("dbt.adapters.trino", reason="dbt-trino not installed")
    if iceberg_lake is None:
        pytest.skip("Docker not available (the dbt gold models run on Trino + Polaris + S3 (SeaweedFS) containers)")
    iceberg_lake.start_trino()
    return iceberg_lake


@pytest.fixture()
def tmp_table(spark):
    """Identifier of a fresh, not yet existing Iceberg table."""
    return f"lakehouse.unit.t_{uuid.uuid4().hex[:12]}"
