from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


def _target_name_in_subprocess(dbt_target: str | None) -> str:
    """Imports dags.dbt_pipeline_dag in a clean subprocess and returns
    PROFILE_CONFIG.target_name — a subprocess import (not monkeypatch +
    importlib.reload) because dags.dbt_pipeline_dag may already be imported
    elsewhere in the pytest session, same pattern as
    tests/test_dag_integrity.py's test_dags_import_with_container_realistic_syspath."""
    env = {**os.environ, "PYTHONPATH": str(REPO_ROOT)}
    if dbt_target is None:
        env.pop("DBT_TARGET", None)
    else:
        env["DBT_TARGET"] = dbt_target
    code = "from dags.dbt_pipeline_dag import PROFILE_CONFIG; print(PROFILE_CONFIG.target_name)"
    result = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip().split('\n')[-1]


def test_dbt_target_defaults_to_dev_when_unset():
    assert _target_name_in_subprocess(None) == "dev"


def test_dbt_target_reads_env_var_when_set():
    assert _target_name_in_subprocess("prod") == "prod"


def test_profiles_yml_has_prod_target_and_env_var_default():
    profiles = yaml.safe_load((REPO_ROOT / "dbt_project" / "profiles.yml").read_text())
    outputs = profiles["dbt_warehouse"]["outputs"]
    assert "prod" in outputs
    assert profiles["dbt_warehouse"]["target"] == "{{ env_var('DBT_TARGET', 'dev') }}"
