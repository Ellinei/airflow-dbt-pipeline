"""
Shared operational defaults for all DAGs in this project: a uniform
retry policy and Slack failure alerting, kept in one place so the
policy can't drift between DAGs (Phase 2 — see
docs/superpowers/specs/2026-07-18-phase2-retries-alerting-idempotency-design.md).
"""
from __future__ import annotations

import os
from datetime import timedelta

import requests


def slack_alert(context: dict) -> None:
    """Post a failure notification to Slack if a webhook URL is configured.
    Fires once per failed task instance, after Airflow's own retries are
    exhausted (Airflow's on_failure_callback semantics) — not per retry
    attempt, to avoid alert noise on transient blips that self-heal."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        return
    ti = context["task_instance"]
    requests.post(webhook_url, json={
        "text": (
            f":red_circle: *Pipeline failure*\n"
            f"*DAG:* {ti.dag_id}  *Task:* {ti.task_id}\n"
            f"*Log:* {ti.log_url}"
        )
    })


def operational_default_args() -> dict:
    """Shared default_args for all 4 DAGs: retries, retry_delay, and Slack
    failure alerting in one place so the policy can't drift between DAGs."""
    return {
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": slack_alert,
    }
