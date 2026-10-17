"""Table registry and job layout (no Spark session needed)."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
import yaml

from common.tables import BRONZE_TOPICS, LAYERS, MONITORING_TABLES, TABLES, select_tables, table_id

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "spark" / "app"
JOBS = APP / "jobs"
DBT = ROOT / "dbt"


def job_file(layer: str, table: str) -> Path:
    matches = [p for p in (JOBS / layer).rglob(f"{table}.py")]
    assert len(matches) == 1, f"expected exactly one {layer} job named {table}.py, found {matches}"
    return matches[0]


def test_layers_and_counts():
    assert LAYERS == ("bronze", "silver", "gold")
    assert {layer: len(tables) for layer, tables in TABLES.items()} == {"bronze": 16, "silver": 14, "gold": 14}


@pytest.mark.parametrize("layer", LAYERS)
def test_identifiers_are_catalog_layer_table(layer):
    for table, ident in TABLES[layer].items():
        assert ident == f"lakehouse.{layer}.{table}"
    assert table_id(layer, next(iter(TABLES[layer]))) in TABLES[layer].values()


def test_monitoring_tables_live_in_the_monitoring_namespace():
    assert set(MONITORING_TABLES.values()) == {"lakehouse.monitoring.pipeline_metrics", "lakehouse.monitoring.data_quality_log"}


def test_bronze_tables_map_to_debezium_topics():
    assert set(BRONZE_TOPICS) == set(TABLES["bronze"])
    assert BRONZE_TOPICS["olist_orders"] == "olist.public.olist_orders"
    assert BRONZE_TOPICS["click_events"] == "clickstream.public.click_events"


def test_polaris_init_creates_every_namespace():
    init = (ROOT / "polaris" / "init.sh").read_text(encoding="utf-8")
    namespaces = init.split('NAMESPACES="', 1)[1].split('"', 1)[0].split()
    assert set(namespaces) == {*LAYERS, "monitoring"}


def test_select_tables():
    assert select_tables("silver") == TABLES["silver"]
    assert list(select_tables("gold", "fact_orders")) == ["fact_orders"]
    with pytest.raises(KeyError):
        select_tables("gold", "nope")


def test_every_silver_table_has_one_job_with_entrypoint():
    for table in TABLES["silver"]:
        tree = ast.parse(job_file("silver", table).read_text(encoding="utf-8"))
        funcs = {n.name for n in tree.body if isinstance(n, ast.FunctionDef)}
        assert "run" in funcs, f"silver/{table}: no run(spark)"
        guards = [n for n in tree.body if isinstance(n, ast.If) and "__main__" in ast.unparse(n.test)]
        assert guards, f"silver/{table}: no __main__ entry point for spark-submit"


def test_silver_jobs_write_their_registry_table():
    """A job must write the table the registry (DQ, maintenance) uses."""
    for table in TABLES["silver"]:
        source = job_file("silver", table).read_text(encoding="utf-8")
        assert f'table_id("silver", "{table}")' in source, f"silver/{table}: job does not write lakehouse.silver.{table}"


def test_every_gold_table_is_a_dbt_model_in_the_catalog():
    """Each gold table is a dbt model of the same name, built in <catalog>.gold with the shared properties."""
    models = {p.stem: p for p in (DBT / "models").rglob("*.sql")}
    assert set(models) == set(TABLES["gold"])
    for table in TABLES["gold"]:
        assert "properties=gold_properties()" in models[table].read_text(encoding="utf-8"), table
    profile = yaml.safe_load((DBT / "profiles.yml").read_text(encoding="utf-8"))["lakehouse"]["outputs"]["prod"]
    assert "'lakehouse'" in profile["database"] and profile["schema"] == "gold"
    assert '"format_version": "2"' in (DBT / "macros" / "keys.sql").read_text(encoding="utf-8")
