from __future__ import annotations

from dags._operational_defaults import operational_default_args, slack_alert


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
        lambda url, json: calls.append((url, json)),
    )

    slack_alert({"task_instance": _FakeTaskInstance()})

    assert len(calls) == 1
    url, payload = calls[0]
    assert url == "https://hooks.slack.example/webhook"
    assert "test_dag" in payload["text"]
    assert "test_task" in payload["text"]
    assert "http://example.com/log" in payload["text"]


def test_operational_default_args_shape():
    from datetime import timedelta

    args = operational_default_args()

    assert args["retries"] == 2
    assert args["retry_delay"] == timedelta(minutes=5)
    assert args["on_failure_callback"] is slack_alert
