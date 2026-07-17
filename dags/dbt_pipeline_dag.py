"""
dbt_pipeline DAG
════════════════
Orchestrates a full dbt run against the warehouse PostgreSQL using
Astronomer Cosmos (https://astronomer.github.io/astronomer-cosmos/).

Pipeline order
──────────────
  ingest_olist ──┐
  dbt_deps  ──►  dbt_seed  ──┤
                             ├──►  [dbt_transform TaskGroup]  ──►  dbt_docs_generate
                                      stg_customers       ──► mart_customer_orders
                                      stg_orders          ──►
                                      stg_olist_*         ──► mart_olist_customer_orders
                                                           ──► mart_olist_seller_performance

Cosmos auto-generates one Airflow task per dbt node (model + test),
giving fine-grained visibility in the Airflow UI.

Schedule: daily at midnight UTC.  Set catchup=False so only the next
scheduled run queues up on first deployment.
"""

from __future__ import annotations

import os
import shutil
from datetime import datetime
from pathlib import Path

import requests

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode

# ── Paths ──────────────────────────────────────────────────────────────────────
# Computed relative to this file rather than hardcoded to the Docker container
# path, so DAG parsing also works in CI (dags/../dbt_project resolves to
# /opt/airflow/dbt_project inside the container — same mount topology — and to
# <repo_root>/dbt_project when running directly from a checkout).
DBT_PROJECT_PATH = Path(__file__).resolve().parent.parent / "dbt_project"
DBT_PROFILES_PATH = DBT_PROJECT_PATH                # profiles.yml lives here
# shutil.which finds dbt on PATH (true in CI, and also true inside the Docker
# container since pip install puts it on the airflow user's PATH); falls back
# to the container's known install location if not found on PATH.
DBT_EXECUTABLE = Path(shutil.which("dbt") or "/home/airflow/.local/bin/dbt")

# ── Olist ingestion paths ──────────────────────────────────────────────────────
OLIST_DATA_DIR = Path("/opt/airflow/data/olist")
OLIST_FILES = {
    "olist_customers_dataset.csv": "olist_customers",
    "olist_orders_dataset.csv": "olist_orders",
    "olist_order_items_dataset.csv": "olist_order_items",
    "olist_order_payments_dataset.csv": "olist_order_payments",
    "olist_order_reviews_dataset.csv": "olist_order_reviews",
    "olist_products_dataset.csv": "olist_products",
    "olist_sellers_dataset.csv": "olist_sellers",
    "olist_geolocation_dataset.csv": "olist_geolocation",
    "product_category_name_translation.csv": "olist_product_category_translation",
}

# Zip-code-prefix columns must stay strings or pandas drops leading zeros on
# Brazilian CEP codes (e.g. "01046" -> 1046).
ZIP_DTYPE_OVERRIDES = {
    "olist_customers_dataset.csv": {"customer_zip_code_prefix": str},
    "olist_sellers_dataset.csv": {"seller_zip_code_prefix": str},
    "olist_geolocation_dataset.csv": {"geolocation_zip_code_prefix": str},
}


def _ingest_olist_files(engine, data_dir: Path, files_map: dict[str, str]) -> dict[str, int]:
    """Load each Olist CSV in files_map into Postgres schema `raw`, truncating
    each table first if it already exists (idempotent — never drops, since
    dbt's stg_olist_* views depend on these tables after the first run, and a
    plain DROP TABLE fails once dependent views exist). Returns
    {table_name: row_count}. Pulled out of the ingest_olist task body so it's
    directly unit-testable with a fixture data_dir."""
    import pandas as pd
    import sqlalchemy

    missing = [f for f in files_map if not (data_dir / f).exists()]
    if missing:
        raise FileNotFoundError(
            f"Missing Olist CSV(s) in {data_dir}: {', '.join(missing)}. "
            "Download the Kaggle 'Brazilian E-Commerce Public Dataset by "
            "Olist' (olistbr/brazilian-ecommerce) and place all 9 files "
            "there — see data/olist/README.md."
        )

    with engine.begin() as conn:
        conn.execute(sqlalchemy.text("CREATE SCHEMA IF NOT EXISTS raw"))

    row_counts = {}
    for filename, table_name in files_map.items():
        df = pd.read_csv(
            data_dir / filename,
            dtype=ZIP_DTYPE_OVERRIDES.get(filename),
        )
        with engine.begin() as conn:
            table_exists = conn.execute(
                sqlalchemy.text(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = 'raw' AND table_name = :table_name"
                ),
                {"table_name": table_name},
            ).fetchone()
            if table_exists:
                conn.execute(sqlalchemy.text(f'TRUNCATE TABLE raw."{table_name}"'))
            df.to_sql(
                table_name,
                con=conn,
                schema="raw",
                if_exists="append",
                index=False,
                chunksize=10_000,
                method="multi",
            )
        row_counts[table_name] = len(df)

    return row_counts


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

    # ── Step 0: ingest the real-world Olist dataset into the raw schema ───────
    @task
    def ingest_olist() -> dict:
        """Thin Airflow wrapper — see _ingest_olist_files for the actual
        loading logic (module-level, independently unit-tested)."""
        import sqlalchemy

        db_user = os.getenv("WAREHOUSE_DB_USER", "warehouse")
        db_password = os.getenv("WAREHOUSE_DB_PASSWORD", "warehouse")
        db_name = os.getenv("WAREHOUSE_DB_NAME", "warehouse")
        engine = sqlalchemy.create_engine(
            f"postgresql+psycopg2://{db_user}:{db_password}@postgres_warehouse:5432/{db_name}"
        )
        return _ingest_olist_files(engine, OLIST_DATA_DIR, OLIST_FILES)

    ingest = ingest_olist()

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
    ingest >> transform


dbt_pipeline()
