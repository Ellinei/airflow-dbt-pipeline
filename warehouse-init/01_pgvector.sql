-- Runs once when the warehouse container is first created (volume is empty).
-- For existing deployments, run manually:
--   psql -h localhost -p 5433 -U warehouse -d warehouse -f warehouse-init/01_pgvector.sql

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS catalog_embeddings (
    id          SERIAL PRIMARY KEY,
    source      TEXT    NOT NULL,  -- 'model' or 'column'
    model_name  TEXT    NOT NULL,
    column_name TEXT,              -- NULL for model-level entries
    description TEXT    NOT NULL,
    embedding   vector(1536),      -- OpenAI text-embedding-3-small dimension
    updated_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS catalog_embeddings_vec_idx
    ON catalog_embeddings
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 10);
