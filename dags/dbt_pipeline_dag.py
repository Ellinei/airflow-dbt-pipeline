"""
dbt_pipeline DAG
════════════════
Orchestrates a full dbt run against the warehouse PostgreSQL using
Astronomer Cosmos (https://astronomer.github.io/astronomer-cosmos/).

Pipeline order
──────────────
  dbt_seed  ──►  [dbt_transform TaskGroup]
                       stg_customers  ──► mart_customer_orders
                       stg_orders     ──►

Cosmos auto-generates one Airflow task per dbt node (model + test),
giving fine-grained visibility in the Airflow UI.

Schedule: daily at midnight UTC.  Set catchup=False so only the next
scheduled run queues up on first deployment.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

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


@dag(
    dag_id="dbt_pipeline",
    description="Seeds raw data then runs and tests all dbt models via Cosmos.",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dbt", "cosmos", "warehouse", "portfolio"],
    doc_md=__doc__,
)
def dbt_pipeline() -> None:

    # ── Step 1: load seeds into the warehouse ─────────────────────────────────
    # `dbt seed` is not handled by Cosmos DbtTaskGroup, so we use BashOperator.
    seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            f"{DBT_EXECUTABLE} seed "
            f"--project-dir {DBT_PROJECT_PATH} "
            f"--profiles-dir {DBT_PROFILES_PATH}"
        ),
    )

    # ── Step 2: run all models + tests via Cosmos ──────────────────────────────
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

    # ── Dependency chain ──────────────────────────────────────────────────────
    seed >> transform


dbt_pipeline()
