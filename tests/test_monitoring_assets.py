"""Grafana monitoring assets stay consistent with the tables they query (no Spark needed)."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

from common.tables import MONITORING_SCHEMA, MONITORING_TABLES

GRAFANA = Path(__file__).resolve().parents[1] / "grafana"
DASHBOARD = GRAFANA / "dashboards" / "pipeline-monitoring.json"
ALERTS = GRAFANA / "provisioning" / "alerting" / "pipeline-alerts.yml"


def _dashboard_queries():
    dash = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    return [t["rawSQL"] for p in dash["panels"] for t in p.get("targets", [])]


def _alert_rules():
    return yaml.safe_load(ALERTS.read_text(encoding="utf-8"))["groups"][0]["rules"]


def test_dashboard_panels_have_unique_ids_and_known_datasource():
    dash = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    ids = [p["id"] for p in dash["panels"]]
    assert len(ids) == len(set(ids))
    datasources = yaml.safe_load((GRAFANA / "provisioning" / "datasources" / "trino.yml").read_text())["datasources"]
    uids = {d["uid"] for d in datasources}
    for panel in dash["panels"]:
        for target in panel.get("targets", []):
            assert target["datasource"]["uid"] in uids


def test_queries_only_read_registered_monitoring_tables():
    allowed = {f"delta.{MONITORING_SCHEMA}.{t}" for t in MONITORING_TABLES}
    queries = _dashboard_queries() + [
        t["model"]["rawSQL"] for r in _alert_rules() for t in r["data"] if "rawSQL" in t["model"]
    ]
    assert queries
    for sql in queries:
        assert set(re.findall(r"delta\.\w+\.\w+", sql)) <= allowed, sql


def test_alert_rules_are_wired_to_the_contact_point():
    alerts = yaml.safe_load(ALERTS.read_text(encoding="utf-8"))
    assert alerts["policies"][0]["receiver"] == alerts["contactPoints"][0]["name"]
    assert alerts["contactPoints"][0]["receivers"][0]["settings"]["url"] == "${ALERT_WEBHOOK_URL}"
    for rule in _alert_rules():
        refs = {step["refId"] for step in rule["data"]}
        assert rule["condition"] in refs
        assert {"A", "B", "C"} <= refs


def test_dashboard_sql_columns_exist_in_the_metrics_schema():
    from common import metrics

    sql = " ".join(_dashboard_queries())
    for column in ("write_mode", "rows_inserted", "rows_updated", "rows_deleted"):
        assert column in metrics._METRICS_SCHEMA
        assert column in sql


def test_alert_webhook_is_passed_to_grafana_and_airflow():
    compose = (GRAFANA.parent / "docker-compose.yml").read_text(encoding="utf-8")
    assert compose.count("ALERT_WEBHOOK_URL") >= 2
    assert "./grafana/provisioning/alerting:/etc/grafana/provisioning/alerting" in compose


@pytest.mark.parametrize("name", ["pipeline_metrics", "data_quality_log"])
def test_monitoring_tables_are_registered(name):
    assert name in MONITORING_TABLES
