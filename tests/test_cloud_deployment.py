from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

from dags.dbt_pipeline_dag import OLIST_FILES, _ensure_olist_data_available

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


def test_dockerfile_bakes_dags_and_dbt_project():
    dockerfile = (REPO_ROOT / "Dockerfile").read_text()
    assert "COPY --chown=airflow:0 dags/ /opt/airflow/dags/" in dockerfile
    assert "COPY --chown=airflow:0 dbt_project/ /opt/airflow/dbt_project/" in dockerfile
    assert re.search(r"^RUN .*dbt deps", dockerfile, re.MULTILINE)
    assert "COPY plugins/" not in dockerfile


def test_ensure_olist_data_skips_download_when_files_present_and_bucket_set(tmp_path):
    for filename in OLIST_FILES:
        (tmp_path / filename).write_text("stub")
    with patch("boto3.client") as mock_client:
        _ensure_olist_data_available(tmp_path, OLIST_FILES, "some-bucket")
    mock_client.assert_not_called()


def test_ensure_olist_data_skips_download_when_bucket_unset(tmp_path):
    with patch("boto3.client") as mock_client:
        _ensure_olist_data_available(tmp_path, OLIST_FILES, None)
    mock_client.assert_not_called()


def test_ensure_olist_data_downloads_when_files_missing_and_bucket_set(tmp_path):
    mock_s3 = MagicMock()
    with patch("boto3.client", return_value=mock_s3) as mock_client:
        _ensure_olist_data_available(tmp_path, OLIST_FILES, "some-bucket")
    mock_client.assert_called_once_with("s3")
    assert mock_s3.download_file.call_count == len(OLIST_FILES)
    called_keys = {call.args[1] for call in mock_s3.download_file.call_args_list}
    assert called_keys == {f"olist-raw/{f}" for f in OLIST_FILES}
    called_dests = {call.args[2] for call in mock_s3.download_file.call_args_list}
    assert called_dests == {str(tmp_path / f) for f in OLIST_FILES}
