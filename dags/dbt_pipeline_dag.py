"""
dbt_pipeline DAG
════════════════
Orchestrates a full dbt run against the warehouse PostgreSQL using
Astronomer Cosmos (https://astronomer.github.io/astronomer-cosmos/).

Pipeline order
──────────────
  dbt_deps  ──►  dbt_seed  ──►  [dbt_transform TaskGroup]  ──►  dbt_docs_generate
                                      stg_customers  ──► mart_customer_orders
                                      stg_orders     ──►

Cosmos auto-generates one Airflow task per dbt node (model + test),
giving fine-grained visibility in the Airflow UI.

Schedule: daily at midnight UTC.  Set catchup=False so only the next
scheduled run queues up on first deployment.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import requests

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode

# ── Paths (inside the Airflow containers) ─────────────────────────────────────
DBT_PROJECT_PATH = Path("/opt/airflow/dbt_project")
DBT_PROFILES_PATH = DBT_PROJECT_PATH                # profiles.yml lives here
DBT_EXECUTABLE = Path("/home/airflow/.local/bin/dbt")

# ── Cosmos profile config ──────────────────────────────────────────────────────
# We use a file-based profile so no Airflow Connection object is required.
# profiles.yml reads credentials from env vars injected by docker-compose.
PROFILE_CONFIG = ProfileConfig(
    profile_name="dbt_warehouse",
    target_name="dev",
    profiles_yml_filepath=DBT_PROFILES_PATH / "profiles.yml",
)

# ── Cosmos execution config ────────────────────────────────────────────────────
EXECUTION_CONFIG = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE,
)

# ── Cosmos project config ──────────────────────────────────────────────────────
PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)


def slack_alert(context: dict) -> None:
    """Post a failure notification to Slack if a webhook URL is configured."""
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


@dag(
    dag_id="dbt_pipeline",
    description="Seeds raw data then runs and tests all dbt models via Cosmos.",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dbt", "cosmos", "warehouse", "portfolio"],
    doc_md=__doc__,
    default_args={"on_failure_callback": slack_alert},
)
def dbt_pipeline() -> None:

    # ── Step 1: resolve dbt packages ─────────────────────────────────────────
    deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"{DBT_EXECUTABLE} deps "
            f"--project-dir {DBT_PROJECT_PATH} "
            f"--profiles-dir {DBT_PROFILES_PATH}"
        ),
    )

    # ── Step 2: load seeds into the warehouse ─────────────────────────────────
    # `dbt seed` is not handled by Cosmos DbtTaskGroup, so we use BashOperator.
    seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            f"{DBT_EXECUTABLE} seed "
            f"--project-dir {DBT_PROJECT_PATH} "
            f"--profiles-dir {DBT_PROFILES_PATH}"
        ),
    )

    # ── Step 3: run all models + tests via Cosmos ──────────────────────────────
    # DbtTaskGroup creates individual tasks for every model node so you can
    # see stg_customers, stg_orders, mart_customer_orders as separate tasks
    # in the Airflow graph view.
    #
    # LoadMode.DBT_LS: Cosmos calls `dbt ls` at scheduler parse time to
    # discover models — accurate and avoids hard-coding node names here.
    transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=["path:models/"],   # only models/ — excludes seeds
            # emit_datasets=False: Cosmos would try to resolve the warehouse
            # Airflow Connection for OpenLineage, but we use a file-based
            # profiles.yml with no Connection — causing an
            # AirflowCompatibilityError even though dbt itself succeeds.
            # Also: dbt 1.8 produces manifest schema v12; bundled dbt-ol
            # only supports ≤v7, so the integration would fail anyway.
            emit_datasets=False,
        ),
    )

    # ── Step 4: regenerate dbt docs so the lineage graph stays current ────────
    docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            f"{DBT_EXECUTABLE} docs generate "
            f"--project-dir {DBT_PROJECT_PATH} "
            f"--profiles-dir {DBT_PROFILES_PATH}"
        ),
    )

    # ── Dependency chain ──────────────────────────────────────────────────────
    deps >> seed >> transform >> docs


dbt_pipeline()
