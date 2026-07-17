from __future__ import annotations

import time

import httpx
import pytest
import sqlalchemy
from openai import APIConnectionError

from dags.rag_index_dag import (
    _create_embeddings_with_retry,
    _ensure_catalog_embeddings_schema,
    _extract_descriptions_from_manifest,
)


def test_extract_descriptions_model_and_column_level():
    manifest = {
        "nodes": {
            "model.dbt_warehouse.stg_customers": {
                "resource_type": "model",
                "name": "stg_customers",
                "description": "Cleaned customer records.",
                "columns": {
                    "customer_id": {"description": "Primary key."},
                    "email": {"description": ""},
                },
            },
            "test.dbt_warehouse.not_null_stg_customers_customer_id": {
                "resource_type": "test",
                "name": "not_null_stg_customers_customer_id",
            },
        }
    }
    docs = _extract_descriptions_from_manifest(manifest)

    assert {"source": "model", "model_name": "stg_customers", "column_name": None,
            "description": "Cleaned customer records."} in docs
    assert {"source": "column", "model_name": "stg_customers", "column_name": "customer_id",
            "description": "stg_customers.customer_id: Primary key."} in docs
    assert not any(d["column_name"] == "email" for d in docs)
    assert len(docs) == 2


def test_extract_descriptions_empty_manifest_returns_empty_list():
    assert _extract_descriptions_from_manifest({"nodes": {}}) == []


def test_create_embeddings_with_retry_succeeds_first_try():
    class _FakeEmbeddings:
        def create(self, model, input):
            return {"model": model, "input": input}

    class _FakeClient:
        embeddings = _FakeEmbeddings()

    result = _create_embeddings_with_retry(_FakeClient(), "text-embedding-3-small", ["a", "b"])

    assert result == {"model": "text-embedding-3-small", "input": ["a", "b"]}


def test_create_embeddings_with_retry_recovers_after_transient_errors(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    request = httpx.Request("POST", "https://api.openai.com/v1/embeddings")
    attempts = {"count": 0}

    class _FakeEmbeddings:
        def create(self, model, input):
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise APIConnectionError(request=request)
            return "success"

    class _FakeClient:
        embeddings = _FakeEmbeddings()

    result = _create_embeddings_with_retry(_FakeClient(), "text-embedding-3-small", ["a"])

    assert result == "success"
    assert attempts["count"] == 3


def test_create_embeddings_with_retry_gives_up_after_max_attempts(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    request = httpx.Request("POST", "https://api.openai.com/v1/embeddings")

    class _FakeEmbeddings:
        def create(self, model, input):
            raise APIConnectionError(request=request)

    class _FakeClient:
        embeddings = _FakeEmbeddings()

    with pytest.raises(APIConnectionError):
        _create_embeddings_with_retry(_FakeClient(), "text-embedding-3-small", ["a"])


def test_catalog_embeddings_upsert_is_idempotent(warehouse_engine):
    """Formalizes the manual verification from the Phase 0 rag_index bug fix:
    rerunning the upsert with changed descriptions updates in place instead
    of duplicating rows."""
    upsert_sql = sqlalchemy.text("""
        INSERT INTO catalog_embeddings
            (source, model_name, column_name, description, embedding, updated_at)
        VALUES
            (:source, :model_name, :column_name, :description, NULL, now())
        ON CONFLICT (source, model_name, COALESCE(column_name, ''))
        DO UPDATE SET
            description = EXCLUDED.description,
            embedding   = EXCLUDED.embedding,
            updated_at  = EXCLUDED.updated_at
    """)
    _ensure_catalog_embeddings_schema(warehouse_engine)

    with warehouse_engine.begin() as conn:
        conn.execute(sqlalchemy.text(
            "DELETE FROM catalog_embeddings WHERE model_name = 'test_model'"
        ))

        conn.execute(upsert_sql, {"source": "column", "model_name": "test_model",
                                   "column_name": "test_col", "description": "first"})
        conn.execute(upsert_sql, {"source": "model", "model_name": "test_model",
                                   "column_name": None, "description": "first model"})
        conn.execute(upsert_sql, {"source": "column", "model_name": "test_model",
                                   "column_name": "test_col", "description": "second"})
        conn.execute(upsert_sql, {"source": "model", "model_name": "test_model",
                                   "column_name": None, "description": "second model"})

        rows = conn.execute(sqlalchemy.text(
            "SELECT source, description FROM catalog_embeddings "
            "WHERE model_name = 'test_model' ORDER BY source"
        )).fetchall()

        conn.execute(sqlalchemy.text(
            "DELETE FROM catalog_embeddings WHERE model_name = 'test_model'"
        ))

    result = {row.source: row.description for row in rows}
    assert result == {"column": "second", "model": "second model"}
