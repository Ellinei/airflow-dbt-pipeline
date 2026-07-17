from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest
import sqlalchemy

REPO_ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = REPO_ROOT / "dbt_project"


@pytest.fixture(scope="session", autouse=True)
def dbt_deps():
    """Install dbt packages (dbt_utils) once per test session. Cosmos's
    LoadMode.DBT_LS shells out to `dbt ls` at DAG-parse time, which fails if
    dbt_project/dbt_packages/ isn't populated yet — mirrors what airflow-init
    does in docker-compose before the scheduler ever parses DAGs."""
    dbt_exe = shutil.which("dbt")
    assert dbt_exe, "dbt not found on PATH — install project requirements first"
    subprocess.run(
        [dbt_exe, "deps", "--project-dir", str(DBT_PROJECT_DIR),
         "--profiles-dir", str(DBT_PROJECT_DIR)],
        check=True,
    )


@pytest.fixture(scope="session")
def warehouse_engine():
    """SQLAlchemy engine for the test warehouse Postgres (CI service
    container, or the local postgres_warehouse — same WAREHOUSE_DB_* env vars
    the DAGs themselves use). Also ensures the `engineer` role exists: dbt's
    grant_select post-hook (dbt_project/macros/grant_select.sql) runs on
    every model build and fails if the role is missing, and a fresh database
    has no roles until governance/setup_roles.sql is run — which this test
    suite intentionally doesn't do (that's a production/manual concern, not
    a CI one)."""
    db_user = os.getenv("WAREHOUSE_DB_USER", "warehouse")
    db_password = os.getenv("WAREHOUSE_DB_PASSWORD", "warehouse")
    db_name = os.getenv("WAREHOUSE_DB_NAME", "warehouse")
    db_host = os.getenv("WAREHOUSE_DB_HOST", "localhost")
    db_port = os.getenv("WAREHOUSE_DB_PORT", "5432")
    engine = sqlalchemy.create_engine(
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text(
            "DO $$ BEGIN CREATE ROLE engineer NOLOGIN; "
            "EXCEPTION WHEN duplicate_object THEN NULL; END $$;"
        ))
    yield engine
    engine.dispose()
