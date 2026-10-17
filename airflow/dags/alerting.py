"""Task failure alerts for the lakehouse DAGs.

Set ``ALERT_WEBHOOK_URL`` to a Slack-compatible incoming webhook (Slack, Mattermost, Discord's
``/slack`` endpoint, ...) and every failed task posts a message there. Unset = alerts are off.
Alerting never raises: a broken webhook must not change the outcome of a pipeline run.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request

logger = logging.getLogger(__name__)


def format_failure(context: dict) -> str:
    ti = context.get("task_instance")
    dag_id = getattr(ti, "dag_id", None) or getattr(context.get("dag"), "dag_id", "?")
    task_id = getattr(ti, "task_id", "?")
    run_id = getattr(ti, "run_id", None) or getattr(context.get("dag_run"), "run_id", "?")
    error = str(context.get("exception") or "unknown error")[:500]
    lines = [f":red_circle: Lakehouse task failed: `{dag_id}.{task_id}`", f"run: `{run_id}`", f"error: {error}"]
    log_url = getattr(ti, "log_url", None)
    if log_url:
        lines.append(f"logs: {log_url}")
    return "\n".join(lines)


def post_alert(text: str, webhook_url: str | None = None) -> bool:
    """POST ``{"text": ...}`` to the webhook. Returns True when delivered, never raises."""
    url = webhook_url if webhook_url is not None else os.environ.get("ALERT_WEBHOOK_URL", "")
    if not url:
        return False
    try:
        request = urllib.request.Request(
            url,
            data=json.dumps({"text": text}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10):
            pass
        return True
    except Exception:
        logger.warning("Failed to deliver pipeline alert", exc_info=True)
        return False


def notify_failure(context: dict) -> None:
    """Airflow ``on_failure_callback``."""
    try:
        post_alert(format_failure(context))
    except Exception:
        logger.warning("Failed to build pipeline alert", exc_info=True)
