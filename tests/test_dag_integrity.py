from __future__ import annotations

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
