from __future__ import annotations

from pathlib import Path

import pytest
import sqlalchemy

from dags.dbt_pipeline_dag import OLIST_FILES, _ingest_olist_files

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "olist_sample"


def test_ingest_preserves_zip_code_leading_zeros(warehouse_engine):
    _ingest_olist_files(warehouse_engine, FIXTURE_DIR, OLIST_FILES)
    with warehouse_engine.connect() as conn:
        zip_value = conn.execute(
            sqlalchemy.text('SELECT customer_zip_code_prefix FROM raw."olist_customers" LIMIT 1')
        ).scalar()
    assert isinstance(zip_value, str)
    assert zip_value.startswith("0")


def test_ingest_is_idempotent_on_rerun(warehouse_engine):
    first = _ingest_olist_files(warehouse_engine, FIXTURE_DIR, OLIST_FILES)
    second = _ingest_olist_files(warehouse_engine, FIXTURE_DIR, OLIST_FILES)
    assert first == second


def test_ingest_raises_on_missing_files(tmp_path, warehouse_engine):
    with pytest.raises(FileNotFoundError):
        _ingest_olist_files(warehouse_engine, tmp_path, OLIST_FILES)
