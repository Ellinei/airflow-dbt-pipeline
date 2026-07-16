-- Runs once when the warehouse container is first created (volume is empty),
-- right after 01_pgvector.sql. For existing deployments (volume already has
-- data), run manually — this script is idempotent and safe to rerun:
--   psql -h localhost -p 5433 -U warehouse -d warehouse -f warehouse-init/02_catalog_embeddings_unique_index.sql
--
-- Fixes rag_index's duplicate-row bug: the DAG's original
-- "ON CONFLICT DO NOTHING" had no conflict target (no unique constraint
-- existed), so every rerun appended a full duplicate set of embedding rows.

-- 1. Dedupe existing rows first (safe no-op if none exist). Ties on
--    updated_at (all rows from one run share the same transaction timestamp)
--    are broken by ctid.
DELETE FROM catalog_embeddings
WHERE ctid IN (
    SELECT ctid FROM (
        SELECT ctid,
               ROW_NUMBER() OVER (
                   PARTITION BY source, model_name, COALESCE(column_name, '')
                   ORDER BY updated_at DESC, ctid DESC
               ) AS rn
        FROM catalog_embeddings
    ) ranked
    WHERE rn > 1
);

-- 2. CREATE UNIQUE INDEX, not ALTER TABLE ADD CONSTRAINT — Postgres has no
--    "ADD CONSTRAINT IF NOT EXISTS" for unique constraints. A unique index is
--    idempotent and is a valid ON CONFLICT inference target.
--    COALESCE(column_name, '') because model-level rows have column_name
--    NULL, and a plain unique index treats every NULL as distinct.
CREATE UNIQUE INDEX IF NOT EXISTS catalog_embeddings_unique_idx
    ON catalog_embeddings (source, model_name, COALESCE(column_name, ''));
