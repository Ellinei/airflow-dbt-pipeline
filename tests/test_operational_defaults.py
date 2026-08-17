from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from airflow.models import DagBag

from dags._operational_defaults import operational_default_args, slack_alert

REPO_ROOT = Path(__file__).resolve().parent.parent
DAGS_DIR = REPO_ROOT / "dags"

ALL_DAG_IDS = {"dbt_pipeline", "mlflow_training", "mlflow_training_olist", "rag_index"}


class _FakeTaskInstance:
    dag_id = "test_dag"
    task_id = "test_task"
    log_url = "http://example.com/log"


def test_slack_alert_noops_without_webhook_url(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    calls = []
    monkeypatch.setattr(
        "dags._operational_defaults.requests.post",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    slack_alert({"task_instance": _FakeTaskInstance()})

    assert calls == []


def test_slack_alert_posts_expected_payload(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.example/webhook")
    calls = []
    monkeypatch.setattr(
        "dags._operational_defaults.requests.post",
        lambda url, json, timeout: calls.append((url, json, timeout)),
    )

    slack_alert({"task_instance": _FakeTaskInstance()})

    assert len(calls) == 1
    url, payload, timeout = calls[0]
    assert url == "https://hooks.slack.example/webhook"
    # A hung webhook must not block the callback: slack_alert is the
    # on_failure_callback for all 4 DAGs, so an untimed POST would have
    # instance-wide blast radius.
    assert timeout == 10
    assert "test_dag" in payload["text"]
    assert "test_task" in payload["text"]
    assert "http://example.com/log" in payload["text"]


def test_operational_default_args_shape():
    from datetime import timedelta

    args = operational_default_args()

    assert args["retries"] == 2
    assert args["retry_delay"] == timedelta(minutes=5)
    assert args["on_failure_callback"] is slack_alert


def test_all_dags_have_max_active_runs_one():
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    for dag_id in ALL_DAG_IDS:
        assert dagbag.dags[dag_id].max_active_runs == 1, dag_id


def test_all_dags_have_shared_retry_policy_in_default_args():
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    for dag_id in ALL_DAG_IDS:
        dag = dagbag.dags[dag_id]
        assert dag.default_args.get("retries") == 2, dag_id
        assert dag.default_args.get("retry_delay") == timedelta(minutes=5), dag_id


def test_all_dags_have_shared_slack_alert_on_failure():
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    for dag_id in ALL_DAG_IDS:
        dag = dagbag.dags[dag_id]
        assert dag.default_args.get("on_failure_callback") is slack_alert, dag_id


def test_dbt_pipeline_tasks_inherit_retries_via_cascading():
    """Cosmos generates one Airflow task per dbt model/test inside
    dbt_pipeline's DbtTaskGroup — this confirms default_args cascades to
    those generated tasks too, not just the directly-defined ones
    (ingest_olist, dbt_deps, dbt_seed, dbt_docs_generate)."""
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    dag = dagbag.dags["dbt_pipeline"]
    assert len(dag.tasks) > 4, "expected Cosmos-generated tasks in addition to the 4 direct ones"
    for task in dag.tasks:
        assert task.retries == 2, task.task_id
