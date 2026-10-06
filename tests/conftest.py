"""Shared test setup.

Spark-backed tests run a local Spark session with Delta against a temp "lake" directory instead
of MinIO: the bucket env vars are set *before* `common.config` is imported, so every job and the
table registry resolve their paths under that directory. They are skipped when Java or
delta-spark is missing; run them with `pip install -r tests/requirements.txt` and a JDK 17+.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "spark" / "app"))

LAKE = Path(tempfile.mkdtemp(prefix="lakehouse_test_"))
for _layer in ("bronze", "silver", "gold"):
    os.environ[f"{_layer.upper()}_BUCKET"] = (LAKE / _layer).as_uri()


def pytest_sessionfinish(session, exitstatus):
    shutil.rmtree(LAKE, ignore_errors=True)


def _java_available() -> bool:
    java_home = os.environ.get("JAVA_HOME")
    return bool(shutil.which("java") or (java_home and (Path(java_home) / "bin").exists()))


@pytest.fixture(scope="session")
def spark():
    if not _java_available():
        pytest.skip("Java not found (Spark tests need a JDK 17+)")
    delta = pytest.importorskip("delta", reason="delta-spark not installed")
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("lakehouse-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.ansi.enabled", "false")  # same as common.config.create_spark_session
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(LAKE / "warehouse"))
    )
    session = delta.pip_utils.configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture()
def tmp_delta(tmp_path):
    """URI of a fresh, empty directory for a throwaway Delta table."""
    return (tmp_path / "table").as_uri()
