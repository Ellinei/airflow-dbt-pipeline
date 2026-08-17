from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from airflow.models import DagBag

REPO_ROOT = Path(__file__).resolve().parent.parent
DAGS_DIR = REPO_ROOT / "dags"

EXPECTED_DAG_IDS = {"dbt_pipeline", "mlflow_training", "mlflow_training_olist", "rag_index"}


def test_no_dag_import_errors():
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert dagbag.import_errors == {}, dagbag.import_errors


def test_expected_dags_present():
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert EXPECTED_DAG_IDS.issubset(set(dagbag.dags.keys()))


def test_every_dag_has_at_least_one_task():
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    for dag_id, dag in dagbag.dags.items():
        assert len(dag.tasks) > 0, f"{dag_id} has no tasks"


def test_docker_compose_sets_pythonpath_for_dags_package_imports():
    """docker-compose.yml mounts only ./dags (never the repo root) into
    /opt/airflow/dags, so `from dags._operational_defaults import ...`
    inside a DAG file needs /opt/airflow on PYTHONPATH to resolve `dags`
    as an importable package parent — Airflow's own prepare_syspath()
    only ever adds /opt/airflow/dags itself, never its parent."""
    import yaml

    compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())
    env = compose["x-airflow-common"]["environment"]
    assert env.get("PYTHONPATH") == "/opt/airflow"


def test_dags_import_with_container_realistic_syspath():
    """Simulates the container's import conditions without needing Docker:
    Airflow's own prepare_syspath() always adds the dags/ directory itself
    to sys.path (confirmed empirically — it never adds the parent), so this
    only needs to add the parent via PYTHONPATH, mirroring what
    docker-compose.yml's PYTHONPATH=/opt/airflow does in the real
    container. -P stops Python from silently prepending the working
    directory to sys.path, which would otherwise mask this exact bug by
    making the repo root importable by accident."""
    env = {**os.environ, "PYTHONPATH": str(REPO_ROOT)}
    code = (
        "from airflow.models import DagBag;"
        f"db = DagBag(dag_folder={str(DAGS_DIR)!r}, include_examples=False);"
        "assert not db.import_errors, db.import_errors"
    )
    subprocess.run([sys.executable, "-P", "-c", code], env=env, check=True)
