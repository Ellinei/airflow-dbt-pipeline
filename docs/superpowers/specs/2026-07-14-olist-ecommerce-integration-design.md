# Olist Brazilian E-Commerce Integration — Design

## Context

The pipeline's existing dbt models (`raw_customers`/`raw_orders` seeds → `stg_customers`/`stg_orders` → `mart_customer_orders`) are a tiny hand-authored demo (~10-15 rows), useful for showing pipeline shape but not for testing the pipeline against anything real-world-scale. The goal here is to add a second, parallel data source — the Kaggle "Olist Brazilian E-Commerce" dataset (`olistbr/brazilian-ecommerce`, ~100k real anonymized orders across 9 CSVs) — so the project can be exercised against real volume and real data-quality mess, without touching or risking the existing demo.

## Decisions (confirmed with user)

1. **Add alongside, don't replace.** Existing seeds/staging/marts/tests/exposure are untouched.
2. **Full 9-table dataset**: customers, orders, order_items, order_payments, order_reviews, products, sellers, geolocation, product_category_name_translation.
3. **Ingestion via a new Airflow task**, not a Postgres init script — keeps "Airflow orchestrates everything" true and demonstrates the EL half of ELT.
4. **Two marts**: `mart_olist_customer_orders` (mirrors the existing customer-orders shape) and `mart_olist_seller_performance` (revenue/reviews by seller) — uses more of the 9 tables meaningfully rather than just customers/orders/payments.
5. **Governance parity now**: `governance/setup_roles.sql` gets equivalent `engineer`/`analyst` grants and RLS for the new schemas in this same pass, not deferred.

## Prior work already on disk (do not redo)

A previous session already created, before this design was finalized:
- `data/olist/README.md` — download instructions for the 9 CSVs.
- `.gitignore` — already has `data/olist/*.csv` ignored.

Everything else below is not yet implemented.

## Architecture

```
data/olist/*.csv (user-downloaded, gitignored)
        │  bind-mounted read-only: ./data → /opt/airflow/data
        ▼
Airflow task `ingest_olist` (pandas + SQLAlchemy)
        │  loads into literal Postgres schema `raw` (raw.olist_*)
        ▼
dbt source('olist_raw', ...) ── first source() usage in this project
        │
   models/olist_staging/*.sql  (9 thin cast-only views, schema public_olist_staging)
        │
   models/olist_marts/*.sql    (2 tables, schema public_olist_marts)
        │
   dbt exposure (Power BI) + governance grants (engineer/analyst)
```

DAG task graph (`dags/dbt_pipeline_dag.py`, same `@dag` function, same `default_args={"on_failure_callback": slack_alert}`):
```
deps ──► seed ──┐
                 ├──► transform (Cosmos DbtTaskGroup, auto-discovers ALL models/) ──► docs
ingest_olist ────┘
```
No changes to the Cosmos wiring itself — `RenderConfig(select=["path:models/"])` already picks up new folders automatically.

## File changes

**New:**
- `dbt_project/models/olist_staging/sources.yml` — one `source: olist_raw`, `schema: raw` (literal, matches what the Python task creates — NOT the same as dbt's own `public_raw` seed schema), 9 `tables:`.
- `dbt_project/models/olist_staging/stg_olist_{customers,orders,order_items,order_payments,order_reviews,products,sellers,geolocation,product_category_translation}.sql` — 9 models, `source`/`renamed` two-CTE style matching `stg_customers.sql`.
- `dbt_project/models/olist_staging/schema.yml` — meta/tags/tests per model, same style as `staging/schema.yml`.
- `dbt_project/models/olist_marts/mart_olist_customer_orders.sql`
- `dbt_project/models/olist_marts/mart_olist_seller_performance.sql`
- `dbt_project/models/olist_marts/schema.yml`

**Modified:**
- `dbt_project/dbt_project.yml` — add `olist_staging` (`+materialized: view`, `+schema: olist_staging`, `+post-hook: grant_select('engineer')`) and `olist_marts` (`+materialized: table`, `+schema: olist_marts`, same post-hook) blocks under `models.dbt_warehouse`.
- `dbt_project/models/marts/exposures.yml` — append a second exposure `olist_ecommerce_dashboard`; existing entry untouched.
- `dags/dbt_pipeline_dag.py` — add `OLIST_DATA_DIR` constant, `OLIST_FILES` mapping, new `@task ingest_olist()`, wire `ingest >> transform` alongside existing `deps >> seed >> transform`.
- `docker-compose.yml` — add `./data:/opt/airflow/data:ro` to `x-airflow-common.volumes`.
- `Dockerfile` — add `"pandas>=2.0.0,<3.0.0"` and `"SQLAlchemy>=2.0.0,<3.0.0"` to the existing pip install block (both currently only present transitively via `mlflow`).
- `governance/setup_roles.sql` — add `engineer` grants (`GRANT USAGE`/`GRANT SELECT ON ALL TABLES`/`ALTER DEFAULT PRIVILEGES`) on `raw`, `public_olist_staging`, `public_olist_marts`; `analyst` grants direct `SELECT` on both new marts (no masked view needed — Olist's customer table has no name/email, only IDs/zip/city/state, so there's materially less PII pressure than the toy demo); RLS mirrored on `mart_olist_customer_orders` only (`engineer_full_access` / an analyst policy scoped to delivered orders), not on the seller-performance mart (aggregate-only, no per-customer sensitivity to gate).

## Key data-modeling facts

- **`customer_unique_id` vs `customer_id`**: Olist mints a new `customer_id` per order; the real repeat-customer key is `customer_unique_id`. `mart_olist_customer_orders` must group by `customer_unique_id`, or every "customer" trivially shows 1 order.
- **IDs are 32-char hex strings**, not ints — cast `::text`, not `::int` (the toy demo's pattern doesn't transfer).
- **No `amount` column on orders** — derived in the mart from `order_payments.payment_value` (or `order_items`), same place the toy demo aggregates, not in staging.
- **Zip prefixes need `dtype=str` on `pd.read_csv`** in the ingestion task, or pandas drops leading zeros on Brazilian CEP codes.
- **`stg_olist_geolocation`** is a deliberate exception to "thin 1:1 staging": raw grain has ~1M rows with duplicate lat/lng per zip prefix; the staging model aggregates to one row per `geolocation_zip_code_prefix`.

## Schema-naming resolution

| Schema | Created by | Value |
|---|---|---|
| `raw` (literal) | Python `ingest_olist` task | Literal string chosen in code |
| `public_raw` | dbt (existing seeds) | dbt's default `generate_schema_name` = `public` + `raw` (no custom macro exists) |
| `public_olist_staging` / `public_olist_marts` | dbt (new models) | Same default macro behavior |

`source()` blocks are never passed through `generate_schema_name` — dbt takes `sources.yml`'s `schema: raw` literally. This must be called out in a comment in `sources.yml` since `raw` vs `public_raw` is an easy copy-paste trap.

## Idempotency / failure behavior

- Each of the 9 tables loaded via its own `engine.begin()` transaction, `to_sql(..., if_exists="replace", chunksize=10_000, method="multi")` — per-table atomic; a mid-load failure rolls back only that table, leaving it at its previous state, not corrupted/empty.
- Fail-fast: check all 9 file paths exist before touching the database; `raise FileNotFoundError` listing exactly which are missing.
- `CREATE SCHEMA IF NOT EXISTS raw` runs once at the top, idempotent by construction.
- `ingest_olist` inherits the DAG-level `on_failure_callback=slack_alert` automatically (same `@dag` function, no override) — verify this empirically, not just by code inspection.
- No resume-from-failure logic — a re-run restarts from table 1. Acceptable at this data volume (full reload takes seconds to low minutes).

## Verification plan

1. Place 9 CSVs in `./data/olist/`, `docker compose up -d --build` (rebuild required).
2. `airflow tasks test dbt_pipeline ingest_olist <date>` — confirm all 9 `raw.olist_*` tables populate with expected row counts.
3. `dbt ls --select source:olist_raw` then `dbt run --select path:models/olist_staging+` then `dbt test --select path:models/olist_staging+` inside the scheduler container.
4. Query `mart_olist_customer_orders` — confirm `total_orders` is not trivially 1 for every row (validates `customer_unique_id` grouping).
5. Query `mart_olist_seller_performance` for sane aggregates (revenue, `avg_review_score` in 1–5).
6. Confirm Airflow graph shows `ingest_olist` plus new nodes inside the existing `dbt_transform` TaskGroup, toy-demo nodes unchanged.
7. `dbt docs generate` succeeds; new source→staging→marts lineage renders.
8. Re-run the full DAG a second time — row counts stable, no doubling, tests still pass (idempotency check).
9. As `engineer_user`/`analyst_user` (from `governance/setup_roles.sql`), confirm the new grants work as expected (engineer sees everything, analyst sees only what's granted).
10. Optional failure-path check: rename one CSV, re-trigger, confirm fail-fast error and (if `SLACK_WEBHOOK_URL` set) a Slack alert.

## Explicitly out of scope for this pass

- dbt source freshness blocks.
- A geolocation-based mart (staged but unconsumed by any mart).
- `product_category_name_translation` folded into a mart (staged, available for ad-hoc/BI joins only).
- Incremental materialization for the new marts (full rebuild each run, matching existing marts).
- MLflow training on Olist data; RAG/pgvector changes beyond the automatic pickup of new model descriptions in `manifest.json` (`rag_index_dag.py` needs no code changes for this).
- Resumable/checkpointed ingestion.
- The optional `assert_olist_payments_match_order_items.sql` reconciliation test — a good future addition to the currently-empty `tests/` folder, not required for v1.
