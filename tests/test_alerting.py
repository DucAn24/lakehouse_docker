"""Airflow failure alerts: message formatting and webhook delivery (no Airflow needed)."""

from __future__ import annotations

import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow" / "dags"))
import alerting  # noqa: E402


@pytest.fixture()
def webhook():
    received: list[dict] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/hook", received
    server.shutdown()


def _context(**overrides):
    ti = SimpleNamespace(dag_id="lakehouse_etl_pipeline", task_id="silver.orders", run_id="run_1", log_url="http://af/log")
    return {"task_instance": ti, "exception": RuntimeError("boom"), **overrides}


def test_format_failure_names_task_run_error_and_logs():
    text = alerting.format_failure(_context())
    assert "lakehouse_etl_pipeline.silver.orders" in text
    assert "run_1" in text
    assert "boom" in text
    assert "http://af/log" in text


def test_format_failure_truncates_long_errors():
    assert len(alerting.format_failure(_context(exception=RuntimeError("x" * 5000)))) < 800


def test_notify_failure_posts_to_the_webhook(monkeypatch, webhook):
    url, received = webhook
    monkeypatch.setenv("ALERT_WEBHOOK_URL", url)
    alerting.notify_failure(_context())
    assert len(received) == 1
    assert "lakehouse_etl_pipeline.silver.orders" in received[0]["text"]


def test_alerts_are_off_without_a_webhook(monkeypatch):
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    assert alerting.post_alert("hi") is False
    alerting.notify_failure(_context())  # must not raise


def test_a_broken_webhook_never_raises(monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "http://127.0.0.1:9/unreachable")
    assert alerting.post_alert("hi") is False
    alerting.notify_failure(_context())
    alerting.notify_failure({})  # malformed context
