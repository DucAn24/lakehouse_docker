"""Static consistency checks for spark/app (no Spark or Airflow needed).

    python scripts/check_spark_layout.py

Fails (exit 1) when:
  - a `from common.<mod> import <name>` does not resolve, or old flat imports (utils, config, ...) remain
  - a DAG references a job file that does not exist
  - a silver/gold table in common/tables.py has no job file
  - the data quality rules (common/quality.py) and the table registry disagree
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "spark" / "app"
DAGS = ROOT / "airflow" / "dags"
problems: list[str] = []


def parse(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


# 1. imports of the shared package resolve ------------------------------------------------
defined: dict[str, set[str]] = {}
for path in (APP / "common").glob("*.py"):
    names: set[str] = set()
    for node in parse(path).body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            names |= {t.id for t in node.targets if isinstance(t, ast.Name)}
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, ast.ImportFrom):
            names |= {a.asname or a.name for a in node.names}
    defined[path.stem] = names

py_files = sorted(APP.rglob("*.py"))
for path in py_files:
    for node in ast.walk(parse(path)):
        if not isinstance(node, ast.ImportFrom) or not node.module:
            continue
        if node.module.startswith("common."):
            mod = node.module.split(".", 1)[1]
            for alias in node.names:
                if alias.name not in defined.get(mod, set()):
                    problems.append(f"{rel(path)}: cannot import {alias.name} from {node.module}")
        elif node.module in ("config", "utils", "pipeline_metrics", "data_quality"):
            problems.append(f"{rel(path)}: old flat import `from {node.module}` (use common.*)")

# 2. every job path referenced by the DAGs exists -----------------------------------------
consts = {
    "SPARK_APP_DIR": APP,
    "JOBS_DIR": APP / "jobs",
    "BRONZE_JOBS": APP / "jobs" / "bronze",
    "SILVER_JOBS": APP / "jobs" / "silver",
    "GOLD_JOBS": APP / "jobs" / "gold",
    "OPS_JOBS": APP / "jobs" / "ops",
}
dag_paths = 0
for dag_file in sorted(DAGS.glob("*.py")):
    for const, rest in re.findall(r'f"\{([A-Z_]+)\}(/[^"{}]+\.py)"', dag_file.read_text(encoding="utf-8")):
        dag_paths += 1
        target = consts[const] / rest.lstrip("/")
        if not target.is_file():
            problems.append(f"{rel(dag_file)}: job not found {rel(target)}")
for layer in ("bronze", "silver", "gold"):
    if not (APP / "jobs" / layer / "register_tables.py").is_file():
        problems.append(f"missing spark/app/jobs/{layer}/register_tables.py")

# 3. table registry <-> job files <-> quality rules ----------------------------------------
registry: dict[str, list[str]] = {}
for node in parse(APP / "common" / "tables.py").body:
    if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name):
        name = node.targets[0].id
        if name in ("_SILVER_TABLES", "_GOLD_TABLES"):
            registry[name] = [e.value for e in node.value.elts]
        elif name == "_BRONZE_TOPICS":
            registry[name] = [k.value for k in node.value.keys]

job_names = {p.stem for p in (APP / "jobs").rglob("*.py")}
for table in registry["_SILVER_TABLES"] + registry["_GOLD_TABLES"]:
    if table not in job_names:
        problems.append(f"common/tables.py: table {table} has no job file under spark/app/jobs/")

quality: dict[str, list[str]] = {}
for node in parse(APP / "common" / "quality.py").body:
    if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", "") == "BRONZE_KEYS":
        quality["bronze"] = [k.value for k in node.value.keys]
    if isinstance(node, ast.FunctionDef) and node.name in ("get_silver_checks", "get_gold_checks"):
        returns = [n for n in ast.walk(node) if isinstance(n, ast.Return) and isinstance(n.value, ast.Dict)]
        quality[node.name.split("_")[1]] = [k.value for k in returns[-1].value.keys]

for layer, key in (("bronze", "_BRONZE_TOPICS"), ("silver", "_SILVER_TABLES"), ("gold", "_GOLD_TABLES")):
    in_quality, in_registry = set(quality.get(layer, [])), set(registry[key])
    if in_quality != in_registry:
        problems.append(
            f"{layer}: tables only in quality rules={sorted(in_quality - in_registry)}, "
            f"only in registry={sorted(in_registry - in_quality)}"
        )

print(
    f"Checked {len(py_files)} python files, {dag_paths} DAG job paths, "
    f"{sum(len(v) for v in registry.values())} registry tables"
)
if problems:
    print("\n".join(f"  - {p}" for p in problems))
    sys.exit(1)
print("OK")
