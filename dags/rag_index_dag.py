"""
rag_index DAG
══════════════
Reads dbt's manifest.json (produced by dbt_docs_generate), extracts model and
column descriptions, generates OpenAI embeddings, and stores them in the
catalog_embeddings pgvector table so the RAG query agent can do semantic search.

Prerequisites
─────────────
1. OPENAI_API_KEY set in .env (leave blank to skip — infrastructure still builds)
2. postgres_warehouse running with pgvector extension enabled
3. dbt_pipeline DAG has run at least once (so target/manifest.json exists)
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

MANIFEST_PATH = Path("/opt/airflow/dbt_project/target/manifest.json")
WAREHOUSE_DSN = "postgresql+psycopg2://warehouse:warehouse@postgres_warehouse:5432/warehouse"


@dag(
    dag_id="rag_index",
    description="Embed dbt catalog descriptions into pgvector for semantic search.",
    start_date=datetime(2024, 1, 1),
    schedule=None,         # trigger manually after dbt_pipeline runs
    catchup=False,
    tags=["rag", "pgvector", "catalog"],
)
def rag_index() -> None:

    @task
    def ensure_schema() -> None:
        """Create extension + table if not already present (idempotent)."""
        import sqlalchemy

        ddl = """
        CREATE EXTENSION IF NOT EXISTS vector;

        CREATE TABLE IF NOT EXISTS catalog_embeddings (
            id          SERIAL PRIMARY KEY,
            source      TEXT NOT NULL,
            model_name  TEXT NOT NULL,
            column_name TEXT,
            description TEXT NOT NULL,
            embedding   vector(1536),
            updated_at  TIMESTAMPTZ DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS catalog_embeddings_vec_idx
            ON catalog_embeddings
            USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 10);
        """
        engine = sqlalchemy.create_engine(WAREHOUSE_DSN)
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text(ddl))
            conn.commit()

    @task
    def extract_descriptions() -> list[dict]:
        """Pull model + column descriptions from the dbt manifest."""
        if not MANIFEST_PATH.exists():
            raise FileNotFoundError(
                f"{MANIFEST_PATH} not found — run the dbt_pipeline DAG first."
            )

        manifest = json.loads(MANIFEST_PATH.read_text())
        docs: list[dict] = []

        for node_id, node in manifest.get("nodes", {}).items():
            if node.get("resource_type") != "model":
                continue
            model_name = node["name"]
            model_desc = node.get("description", "").strip()
            if model_desc:
                docs.append(
                    {"source": "model", "model_name": model_name,
                     "column_name": None, "description": model_desc}
                )
            for col_name, col_meta in node.get("columns", {}).items():
                col_desc = col_meta.get("description", "").strip()
                if col_desc:
                    docs.append(
                        {"source": "column", "model_name": model_name,
                         "column_name": col_name,
                         "description": f"{model_name}.{col_name}: {col_desc}"}
                    )

        return docs

    @task
    def embed_and_store(docs: list[dict]) -> int:
        """Generate embeddings and upsert into catalog_embeddings."""
        api_key = os.getenv("OPENAI_API_KEY", "")
        if not api_key:
            print("OPENAI_API_KEY not set — skipping embedding step.")
            print(f"Would have embedded {len(docs)} documents.")
            return 0

        import sqlalchemy
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        engine = sqlalchemy.create_engine(WAREHOUSE_DSN)

        upsert_sql = sqlalchemy.text("""
            INSERT INTO catalog_embeddings
                (source, model_name, column_name, description, embedding, updated_at)
            VALUES
                (:source, :model_name, :column_name, :description,
                 :embedding::vector, now())
            ON CONFLICT DO NOTHING
        """)

        stored = 0
        texts = [d["description"] for d in docs]

        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=texts,
        )

        with engine.connect() as conn:
            for doc, emb_obj in zip(docs, response.data):
                vec_str = "[" + ",".join(str(x) for x in emb_obj.embedding) + "]"
                conn.execute(upsert_sql, {**doc, "embedding": vec_str})
                stored += 1
            conn.commit()

        print(f"Stored {stored} embeddings in catalog_embeddings.")
        return stored

    setup = ensure_schema()
    docs = extract_descriptions()
    store = embed_and_store(docs)

    setup >> docs >> store


rag_index()
