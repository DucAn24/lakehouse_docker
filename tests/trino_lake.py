"""Throwaway MinIO + Trino (Docker) that run the dbt gold models over the test lake.

Trino's Delta connector writes only to object storage (it has no transaction log synchronizer for
local paths), so silver is copied from the local test lake into MinIO, dbt builds gold there, and
gold is copied back for the Spark assertions and the gold DQ gate. Trino runs with the repo's own
trino/config.properties and trino/catalog/delta.properties; the containers answer to the stack's
host names (minio, trino) on a private network. Files move with `docker cp` (no bind mounts), so
this also works when the tests themselves run in a container; set TRINO_LAKE_HOST to the address
that reaches published ports from there (Docker Desktop: host.docker.internal).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
import urllib.request
import uuid
from pathlib import Path

from common.catalog import trino_execute

ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT = ROOT / "dbt"
TRINO_IMAGE = "trinodb/trino:483"  # same as docker-compose.yml
MINIO_IMAGE = "minio/minio:latest"
MC_IMAGE = "minio/mc:latest"
MINIO_USER, MINIO_PASSWORD = "admin", "password123"  # hard-coded in trino/catalog/delta.properties
BUCKETS = ("silver", "gold", "trino-metastore")


def docker_available() -> bool:
    return bool(shutil.which("docker")) and subprocess.run(["docker", "info"], capture_output=True).returncode == 0


def _docker(*args: str) -> str:
    proc = subprocess.run(["docker", *args], capture_output=True, text=True)
    if proc.returncode:
        raise RuntimeError(f"docker {' '.join(args)} failed:\n{proc.stderr}")
    return proc.stdout.strip()


class TrinoLake:
    def __init__(self, lake: Path):
        self.lake = lake
        self.name = f"lakehouse-test-{uuid.uuid4().hex[:8]}"
        self.url = ""

    def start(self) -> None:
        minio, trino, mc = (f"{self.name}-{svc}" for svc in ("minio", "trino", "mc"))
        _docker("network", "create", self.name)
        _docker(
            "run", "-d", "--name", minio, "--network", self.name, "--network-alias", "minio",
            "-e", f"MINIO_ROOT_USER={MINIO_USER}", "-e", f"MINIO_ROOT_PASSWORD={MINIO_PASSWORD}",
            MINIO_IMAGE, "server", "/data",
        )  # fmt: skip
        _docker(
            "create", "--name", trino, "--network", self.name, "--network-alias", "trino",
            "-p", "127.0.0.1::8080", TRINO_IMAGE,
        )  # fmt: skip
        _docker("cp", str(ROOT / "trino" / "config.properties"), f"{trino}:/etc/trino/config.properties")
        _docker("cp", str(ROOT / "trino" / "catalog" / "delta.properties"), f"{trino}:/etc/trino/catalog/delta.properties")
        _docker("start", trino)
        # mc client with alias `m` = MinIO; /lake inside it stages files copied to / from the test lake
        _docker("run", "-d", "--name", mc, "--network", self.name, "--entrypoint", "sh", MC_IMAGE, "-c", "sleep infinity")
        _docker(
            "exec", mc, "sh", "-c",
            f"until mc alias set m http://minio:9000 {MINIO_USER} {MINIO_PASSWORD} >/dev/null; do sleep 1; done",
        )  # fmt: skip
        self._mc("mb", "--ignore-existing", *(f"m/{bucket}" for bucket in BUCKETS))
        published = _docker("port", trino, "8080/tcp").splitlines()[0]  # 127.0.0.1:<port>
        host, port = published.rsplit(":", 1)
        self.url = f"http://{os.environ.get('TRINO_LAKE_HOST', host)}:{port}"
        self._wait_for_trino()

    def stop(self) -> None:
        containers = [f"{self.name}-{svc}" for svc in ("mc", "trino", "minio")]
        subprocess.run(["docker", "rm", "-f", *containers], capture_output=True)
        subprocess.run(["docker", "network", "rm", self.name], capture_output=True)

    def _mc(self, *args: str) -> None:
        _docker("exec", f"{self.name}-mc", "mc", *args)

    def _wait_for_trino(self, timeout: int = 180) -> None:
        deadline = time.time() + timeout
        while True:
            try:
                with urllib.request.urlopen(f"{self.url}/v1/info", timeout=5) as resp:
                    if b'"starting":false' in resp.read():
                        self.execute("SELECT 1")
                        return
            except OSError:
                pass
            if time.time() > deadline:
                logs = subprocess.run(
                    ["docker", "logs", "--tail", "40", f"{self.name}-trino"], capture_output=True, text=True
                )
                raise TimeoutError(f"Trino at {self.url} not ready after {timeout}s:\n{logs.stdout}{logs.stderr}")
            time.sleep(2)

    def execute(self, sql: str) -> list[list]:
        return trino_execute(sql, self.url)

    def upload(self, path: str) -> None:
        """Copy <lake>/<path> to s3://<path> (the first path segment is the bucket)."""
        _docker("exec", f"{self.name}-mc", "mkdir", "-p", f"/lake/{path}")
        _docker("cp", f"{self.lake / path}{os.sep}.", f"{self.name}-mc:/lake/{path}")
        self._mc("mirror", "--overwrite", f"/lake/{path}", f"m/{path}")

    def download(self, path: str) -> None:
        """Copy s3://<path> back to <lake>/<path>."""
        self._mc("mirror", "--overwrite", f"m/{path}", f"/lake/{path}")
        (self.lake / path).mkdir(parents=True, exist_ok=True)
        _docker("cp", f"{self.name}-mc:/lake/{path}/.", str(self.lake / path))

    def register(self, schema: str, table: str, location: str) -> None:
        self.execute(f"CREATE SCHEMA IF NOT EXISTS delta.{schema}")
        self.execute(
            f"CALL delta.system.register_table(schema_name => '{schema}', table_name => '{table}', "
            f"table_location => '{location}')"
        )

    def dbt_build(self, work_dir: Path) -> None:
        """`dbt build` the gold project against this Trino; raises with the failed nodes."""
        from dbt.cli.main import dbtRunner

        host, port = self.url.removeprefix("http://").rsplit(":", 1)
        saved = {key: os.environ.get(key) for key in ("TRINO_HOST", "TRINO_PORT")}
        os.environ.update(TRINO_HOST=host, TRINO_PORT=port)
        try:
            run = dbtRunner().invoke(
                [
                    "build",
                    "--project-dir", str(DBT_PROJECT),
                    "--profiles-dir", str(DBT_PROJECT),
                    "--target-path", str(work_dir / "target"),
                    "--log-path", str(work_dir / "logs"),
                ]  # fmt: skip
            )
        finally:
            for key, value in saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
        if not run.success:
            failed = [
                f"{r.node.unique_id}: {r.status} {r.message}"
                for r in (run.result or [])
                if str(r.status) not in ("success", "pass")
            ]
            raise AssertionError(f"dbt build failed: {run.exception or ''}\n" + "\n".join(failed))
