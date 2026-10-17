"""Throwaway S3 (SeaweedFS) + Polaris (+ Trino on demand) in Docker: the stack's Iceberg catalog for the tests.

Same setup as docker-compose.yml: Polaris (in-memory persistence, root credentials bootstrapped
from the environment) is configured by the repo's own polaris/init.sh, and Trino runs with the repo's
trino/config.properties and trino/catalog/lakehouse.properties. Containers answer to the stack's host
names (s3, polaris, trino) on a private network, so the local Spark session (via published ports)
and Trino/dbt share one catalog and one bucket — no files are copied between engines.
Files move into containers with `docker cp` (no bind mounts), so this also works when the tests
themselves run in a container; set LAKE_HOST to the address that reaches published ports from
there (Docker Desktop: host.docker.internal).
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
# same images as docker-compose.yml
TRINO_IMAGE = "trinodb/trino:483"
POLARIS_IMAGE = "apache/polaris:1.7.0"
S3_IMAGE = "chrislusf/seaweedfs:4.48"
CURL_IMAGE = "curlimages/curl:latest"
S3_USER, S3_PASSWORD = "admin", "password123"
POLARIS_ID, POLARIS_SECRET = "root", "s3cr3t"
REGION = "us-east-1"
CATALOG = "lakehouse"

# Environment of the containers that talk to the catalog (same names as x-common-env in docker-compose.yml)
NETWORK_ENV = {
    "POLARIS_URI": "http://polaris:8181/api/catalog",
    "POLARIS_HOST_URL": "http://polaris:8181",
    "POLARIS_CLIENT_ID": POLARIS_ID,
    "POLARIS_CLIENT_SECRET": POLARIS_SECRET,
    "ICEBERG_CATALOG": CATALOG,
    "S3_ENDPOINT": "http://s3:8333",
    "AWS_ACCESS_KEY_ID": S3_USER,
    "AWS_SECRET_ACCESS_KEY": S3_PASSWORD,
    "AWS_REGION": REGION,
}


def docker_available() -> bool:
    return bool(shutil.which("docker")) and subprocess.run(["docker", "info"], capture_output=True).returncode == 0


def _docker(*args: str) -> str:
    proc = subprocess.run(["docker", *args], capture_output=True, text=True)
    if proc.returncode:
        raise RuntimeError(f"docker {' '.join(args)} failed:\n{proc.stdout}{proc.stderr}")
    return proc.stdout.strip()


def _env_args(env: dict[str, str]) -> list[str]:
    return [arg for key, value in env.items() for arg in ("-e", f"{key}={value}")]


def _wait(url: str, ready, what: str, container: str, timeout: int = 180) -> None:
    deadline = time.time() + timeout
    while True:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                if ready(resp.read()):
                    return
        except OSError:
            pass
        if time.time() > deadline:
            logs = subprocess.run(["docker", "logs", "--tail", "40", container], capture_output=True, text=True)
            raise TimeoutError(f"{what} at {url} not ready after {timeout}s:\n{logs.stdout}{logs.stderr}")
        time.sleep(2)


class IcebergLake:
    def __init__(self):
        self.name = f"lakehouse-test-{uuid.uuid4().hex[:8]}"
        self.host = ""
        self.s3_url = self.polaris_url = self.url = ""
        self._containers: list[str] = []

    def _published(self, container: str, port: int) -> str:
        host, published = _docker("port", container, f"{port}/tcp").splitlines()[0].rsplit(":", 1)
        self.host = os.environ.get("LAKE_HOST", host)
        return f"http://{self.host}:{published}"

    def _run(self, svc: str, *args: str) -> str:
        container = f"{self.name}-{svc}"
        _docker("run", "-d", "--name", container, "--network", self.name, "--network-alias", svc, *args)
        self._containers.append(container)
        return container

    def start(self) -> None:
        _docker("network", "create", self.name)
        # SeaweedFS `weed mini` creates the S3 keys and the bucket from the environment
        s3 = self._run(
            "s3", "-p", "127.0.0.1::8333",
            "-e", f"AWS_ACCESS_KEY_ID={S3_USER}", "-e", f"AWS_SECRET_ACCESS_KEY={S3_PASSWORD}",
            "-e", f"S3_BUCKET={CATALOG}",
            S3_IMAGE, "mini", "-dir=/data",
        )  # fmt: skip
        polaris = self._run(
            "polaris", "-p", "127.0.0.1::8181", "-p", "127.0.0.1::8182",
            "-e", f"POLARIS_BOOTSTRAP_CREDENTIALS=POLARIS,{POLARIS_ID},{POLARIS_SECRET}",
            "-e", "POLARIS_REALM_CONTEXT_REALMS=POLARIS",
            "-e", "QUARKUS_OTEL_SDK_DISABLED=true",
            "-e", "polaris.readiness.ignore-severe-issues=true",
            "-e", f"AWS_ACCESS_KEY_ID={S3_USER}", "-e", f"AWS_SECRET_ACCESS_KEY={S3_PASSWORD}",
            "-e", f"AWS_REGION={REGION}",
            POLARIS_IMAGE,
        )  # fmt: skip
        self.s3_url = self._published(s3, 8333)
        self.polaris_url = self._published(polaris, 8181)
        _wait(f"{self.s3_url}/healthz", lambda body: True, "SeaweedFS S3", s3)
        _wait(f"{self._published(polaris, 8182)}/q/health", lambda body: b'"UP"' in body, "Polaris", polaris)

        # the stack's own init script: catalog on s3://lakehouse/, grants, namespaces
        init = f"{self.name}-polaris-init"
        _docker("create", "--name", init, "--network", self.name, *_env_args(NETWORK_ENV),
                "--entrypoint", "sh", CURL_IMAGE, "/tmp/init.sh")  # fmt: skip
        try:
            _docker("cp", str(ROOT / "polaris" / "init.sh"), f"{init}:/tmp/init.sh")
            _docker("start", "-a", init)
            if _docker("inspect", "-f", "{{.State.ExitCode}}", init) != "0":
                raise RuntimeError(f"polaris/init.sh failed:\n{_docker('logs', init)}")
        finally:
            subprocess.run(["docker", "rm", "-f", init], capture_output=True)

    def start_trino(self) -> None:
        if self.url:
            return
        trino = f"{self.name}-trino"
        _docker("create", "--name", trino, "--network", self.name, "--network-alias", "trino",
                "-p", "127.0.0.1::8080", *_env_args(NETWORK_ENV), TRINO_IMAGE)  # fmt: skip
        self._containers.append(trino)
        _docker("cp", str(ROOT / "trino" / "config.properties"), f"{trino}:/etc/trino/config.properties")
        _docker("cp", str(ROOT / "trino" / "catalog" / "lakehouse.properties"), f"{trino}:/etc/trino/catalog/lakehouse.properties")
        _docker("start", trino)
        self.url = self._published(trino, 8080)
        _wait(f"{self.url}/v1/info", lambda body: b'"starting":false' in body, "Trino", trino)
        self.execute("SELECT 1")

    def stop(self) -> None:
        subprocess.run(["docker", "rm", "-f", *reversed(self._containers)], capture_output=True)
        subprocess.run(["docker", "network", "rm", self.name], capture_output=True)

    def spark_env(self) -> dict[str, str]:
        """Env for common.config.iceberg_catalog_conf: the local Spark session reaches Polaris and S3
        through their published ports."""
        return {
            **NETWORK_ENV,
            "ICEBERG_CATALOG_TYPE": "rest",
            "POLARIS_URI": f"{self.polaris_url}/api/catalog",
            "S3_ENDPOINT": self.s3_url,
        }

    def execute(self, sql: str) -> list[list]:
        return trino_execute(sql, self.url)

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
