from __future__ import annotations

import os
import re
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


def test_core_service_ports_are_templated():
    compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())
    services = compose["services"]
    assert services["postgres_airflow"]["ports"] == ["${AIRFLOW_DB_PORT:-5432}:5432"]
    assert services["postgres_warehouse"]["ports"] == ["${WAREHOUSE_DB_PORT:-5433}:5432"]
    assert services["airflow-webserver"]["ports"] == ["${AIRFLOW_WEBSERVER_PORT:-8080}:8080"]
    assert services["mlflow-server"]["ports"] == ["${MLFLOW_PORT:-5000}:5000"]


CORE_SERVICES_WITHOUT_CONTAINER_NAME = {
    "postgres_airflow",
    "postgres_warehouse",
    "airflow-init",
    "airflow-webserver",
    "airflow-scheduler",
}


def test_core_services_have_no_container_name():
    """Only these five (plus mlflow-server, checked separately) must lose
    container_name — OpenMetadata's five services intentionally keep theirs:
    they're dev-only/opt-in in both stacks and never run in prod, so no
    cross-project collision is possible."""
    compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())
    services = compose["services"]
    for service_name in CORE_SERVICES_WITHOUT_CONTAINER_NAME:
        assert "container_name" not in services[service_name], service_name


def test_dbt_target_forwarded_to_containers():
    compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())
    env = compose["x-airflow-common"]["environment"]
    assert env["DBT_TARGET"] == "${DBT_TARGET:-dev}"
    assert "WAREHOUSE_DB_PORT" not in env
    assert "WAREHOUSE_DB_HOST" not in env


def test_mlflow_server_has_both_profiles():
    compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())
    profiles = compose["services"]["mlflow-server"]["profiles"]
    assert set(profiles) == {"mlops", "prod-core"}
    assert "container_name" not in compose["services"]["mlflow-server"]


ENV_VAR_NAME_RE = re.compile(r"^([A-Z_][A-Z0-9_]*)=", re.MULTILINE)


def _env_var_names(path: Path) -> set[str]:
    return set(ENV_VAR_NAME_RE.findall(path.read_text()))


def test_env_prod_example_has_same_variable_names_as_env_example():
    dev_names = _env_var_names(REPO_ROOT / ".env.example")
    prod_names = _env_var_names(REPO_ROOT / ".env.prod.example")
    assert prod_names == dev_names


def test_gitignore_ignores_env_prod_literally():
    gitignore_lines = (REPO_ROOT / ".gitignore").read_text().splitlines()
    assert ".env.prod" in gitignore_lines
    assert not any(line.strip() == ".env.*" for line in gitignore_lines)
