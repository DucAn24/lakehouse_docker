"""Table registry and job layout (no Spark session needed)."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from common.tables import LAYERS, TABLES, select_tables, table_path

APP = Path(__file__).resolve().parents[1] / "spark" / "app"
JOBS = APP / "jobs"


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


@pytest.mark.parametrize("layer", ["silver", "gold"])
def test_every_table_has_one_job_with_entrypoint(layer):
    for table in TABLES[layer]:
        tree = ast.parse(job_file(layer, table).read_text(encoding="utf-8"))
        funcs = {n.name for n in tree.body if isinstance(n, ast.FunctionDef)}
        assert "run" in funcs, f"{layer}/{table}: no run(spark)"
        guards = [n for n in tree.body if isinstance(n, ast.If) and "__main__" in ast.unparse(n.test)]
        assert guards, f"{layer}/{table}: no __main__ entry point for spark-submit"


@pytest.mark.parametrize("layer", ["silver", "gold"])
def test_job_paths_match_registry(layer):
    """The path a job writes must be the path the registry (catalog, DQ, VACUUM) uses."""
    for table, path in TABLES[layer].items():
        source = job_file(layer, table).read_text(encoding="utf-8")
        suffix = path.rsplit("/", 2)[-2]  # table folder name
        assert f"/{suffix}/" in source, f"{layer}/{table}: job does not write to {suffix}/"


@pytest.mark.parametrize("layer", LAYERS)
def test_register_tables_job_exists(layer):
    assert (JOBS / layer / "register_tables.py").is_file()
