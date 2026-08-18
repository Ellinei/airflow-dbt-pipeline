from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_requirements_pins_amazon_provider():
    requirements = (REPO_ROOT / "requirements.txt").read_text()
    assert "apache-airflow-providers-amazon==8.20.0" in requirements


def _warehouse_engine_url_in_subprocess(env_overrides: dict[str, str]) -> str:
    """Imports dags.dbt_pipeline_dag in a clean subprocess and calls
    _warehouse_engine_url() — same pattern as
    tests/test_environment_separation.py's _target_name_in_subprocess, needed
    because dags.dbt_pipeline_dag may already be imported elsewhere in the
    pytest session."""
    env = {**os.environ, "PYTHONPATH": str(REPO_ROOT)}
    for key in ("WAREHOUSE_DB_HOST", "WAREHOUSE_DB_PORT"):
        env.pop(key, None)
    env.update(env_overrides)
    code = "from dags.dbt_pipeline_dag import _warehouse_engine_url; print(_warehouse_engine_url())"
    result = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip().split("\n")[-1]


def test_warehouse_engine_url_falls_back_to_compose_service_name_when_unset():
    url = _warehouse_engine_url_in_subprocess({})
    assert "@postgres_warehouse:5432/" in url


def test_warehouse_engine_url_uses_rds_host_and_port_when_set():
    url = _warehouse_engine_url_in_subprocess({
        "WAREHOUSE_DB_HOST": "my-rds-endpoint.rds.amazonaws.com",
        "WAREHOUSE_DB_PORT": "5433",
    })
    assert "@my-rds-endpoint.rds.amazonaws.com:5433/" in url
