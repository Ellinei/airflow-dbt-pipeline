from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from dags.dbt_pipeline_dag import OLIST_FILES, _ingest_olist_files

REPO_ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = REPO_ROOT / "dbt_project"
OLIST_SAMPLE_DIR = Path(__file__).resolve().parent / "fixtures" / "olist_sample"


def _run_dbt(*args: str) -> None:
    dbt_exe = shutil.which("dbt")
    assert dbt_exe, "dbt not found on PATH — install project requirements first"
    subprocess.run(
        [dbt_exe, *args, "--project-dir", str(DBT_PROJECT_DIR),
         "--profiles-dir", str(DBT_PROJECT_DIR)],
        check=True,
    )


def test_dbt_seed_run_test_succeed_against_toy_demo_and_olist_sample(warehouse_engine):
    _ingest_olist_files(warehouse_engine, OLIST_SAMPLE_DIR, OLIST_FILES)
    _run_dbt("seed")
    _run_dbt("run")
    _run_dbt("test")
