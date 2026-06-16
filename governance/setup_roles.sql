-- ══════════════════════════════════════════════════════════════════════════════
--  Policy as Code — role creation, grants, and row-level security
--  Run once against the warehouse database:
--    docker-compose exec postgres_warehouse \
--      psql -U warehouse -d warehouse -f /docker-entrypoint-initdb.d/setup_roles.sql
--  Or from host:
--    psql -h localhost -p 5433 -U warehouse -d warehouse -f governance/setup_roles.sql
-- ══════════════════════════════════════════════════════════════════════════════

-- ── 1. Roles ──────────────────────────────────────────────────────────────────
-- engineer: full read access to all schemas, sees raw PII
-- analyst:  read access only to the masked mart view, email is hidden

CREATE ROLE IF NOT EXISTS engineer NOLOGIN;
CREATE ROLE IF NOT EXISTS analyst  NOLOGIN;

-- Demo login users (for manual psql verification)
DO $$ BEGIN
  CREATE USER engineer_user WITH PASSWORD 'engineer' IN ROLE engineer;
EXCEPTION WHEN duplicate_object THEN RAISE NOTICE 'engineer_user already exists';
END $$;

DO $$ BEGIN
  CREATE USER analyst_user WITH PASSWORD 'analyst' IN ROLE analyst;
EXCEPTION WHEN duplicate_object THEN RAISE NOTICE 'analyst_user already exists';
END $$;

-- ── 2. Schema-level grants ────────────────────────────────────────────────────
-- engineer: can see raw, staging, and marts
GRANT USAGE ON SCHEMA public_raw     TO engineer;
GRANT USAGE ON SCHEMA public_staging TO engineer;
GRANT USAGE ON SCHEMA public_marts   TO engineer;

-- analyst: only the marts schema (via the masked view)
GRANT USAGE ON SCHEMA public_marts TO analyst;

-- ── 3. Table / view grants ────────────────────────────────────────────────────
-- engineer sees everything in all schemas
GRANT SELECT ON ALL TABLES IN SCHEMA public_raw     TO engineer;
GRANT SELECT ON ALL TABLES IN SCHEMA public_staging TO engineer;
GRANT SELECT ON ALL TABLES IN SCHEMA public_marts   TO engineer;

-- Ensure future tables created by dbt are also accessible
ALTER DEFAULT PRIVILEGES IN SCHEMA public_raw     GRANT SELECT ON TABLES TO engineer;
ALTER DEFAULT PRIVILEGES IN SCHEMA public_staging GRANT SELECT ON TABLES TO engineer;
ALTER DEFAULT PRIVILEGES IN SCHEMA public_marts   GRANT SELECT ON TABLES TO engineer;

-- analyst ONLY sees the masked mart view — no access to raw PII tables
GRANT SELECT ON public_marts.mart_customer_orders_masked TO analyst;

-- ── 4. Row-level security on mart_customer_orders ─────────────────────────────
-- The mart is a TABLE (materialized), so RLS is possible.
-- engineer: all rows; analyst: only customers with at least one completed order.
-- (The analyst role has no direct grant on this table, but policies are added
--  as defence-in-depth so accidental grants never expose full data.)

ALTER TABLE public_marts.mart_customer_orders ENABLE ROW LEVEL SECURITY;

-- Table owner (warehouse user) bypasses RLS by default — allow dbt to write freely.
-- FORCE ROW LEVEL SECURITY would even restrict the owner; omit for dbt compatibility.

DO $$ BEGIN
  CREATE POLICY engineer_full_access
    ON public_marts.mart_customer_orders
    FOR SELECT TO engineer
    USING (true);
EXCEPTION WHEN duplicate_object THEN RAISE NOTICE 'engineer_full_access already exists';
END $$;

DO $$ BEGIN
  CREATE POLICY analyst_completed_only
    ON public_marts.mart_customer_orders
    FOR SELECT TO analyst
    USING (completed_orders > 0);
EXCEPTION WHEN duplicate_object THEN RAISE NOTICE 'analyst_completed_only already exists';
END $$;
