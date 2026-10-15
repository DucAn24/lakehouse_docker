"""Table registry and job layout (no Spark session needed)."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
import yaml

from common.tables import LAYERS, TABLES, select_tables, table_path

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
def test_paths_are_unique_and_end_with_slash(layer):
    paths = list(TABLES[layer].values())
    assert len(paths) == len(set(paths))
    assert all(p.endswith("/") for p in paths)


def test_bronze_paths_use_debezium_topic_names():
    assert table_path("bronze", "olist_orders").endswith("/olist.public.olist_orders/")
    assert table_path("bronze", "click_events").endswith("/clickstream.public.click_events/")


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


def test_silver_job_paths_match_registry():
    """The path a job writes must be the path the registry (catalog, DQ, VACUUM) uses."""
    for table, path in TABLES["silver"].items():
        source = job_file("silver", table).read_text(encoding="utf-8")
        suffix = path.rsplit("/", 2)[-2]  # table folder name
        assert f"/{suffix}/" in source, f"silver/{table}: job does not write to {suffix}/"


def test_every_gold_table_is_a_dbt_model_at_its_registry_path():
    """dbt pins each model to <gold_location>/<model name> (macro gold_properties), the registry's gold path."""
    gold_location = yaml.safe_load((DBT / "dbt_project.yml").read_text(encoding="utf-8"))["vars"]["gold_location"]
    assert gold_location == "s3://gold"  # same bucket as common.config.GOLD_BUCKET (s3a://gold)
    models = {p.stem: p for p in (DBT / "models").rglob("*.sql")}
    assert set(models) == set(TABLES["gold"])
    for table, path in TABLES["gold"].items():
        assert path.rstrip("/").rsplit("/", 1)[-1] == table
        assert "properties=gold_properties()" in models[table].read_text(encoding="utf-8"), table


@pytest.mark.parametrize("layer", LAYERS)
def test_register_tables_job_exists(layer):
    assert (JOBS / layer / "register_tables.py").is_file()
