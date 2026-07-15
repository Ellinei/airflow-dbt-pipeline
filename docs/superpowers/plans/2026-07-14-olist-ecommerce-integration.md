# Olist E-Commerce Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a second, real-world-scale ELT pipeline (Kaggle Olist Brazilian E-Commerce, ~100k orders across 9 tables) running alongside the existing toy dbt demo, without modifying any existing file.

**Architecture:** A new Airflow task (`ingest_olist`, pandas + SQLAlchemy) loads 9 CSVs from a new `./data` bind mount into a literal Postgres `raw` schema, ahead of the existing Cosmos `dbt_transform` task group. New dbt `models/olist_staging/` (9 views, via the project's first `source()` block) and `models/olist_marts/` (2 tables) are auto-discovered by Cosmos's existing `LoadMode.DBT_LS` config — no Cosmos/DAG-wiring changes beyond adding the one new task.

**Tech Stack:** Airflow 2.9.1 (TaskFlow API), dbt-core 1.8.0 + dbt-postgres, Astronomer Cosmos 1.4.3, PostgreSQL 15 (pgvector image), pandas, SQLAlchemy, psycopg2.

## Global Constraints

- Do not modify: `dbt_project/seeds/*`, `dbt_project/models/staging/*`, `dbt_project/models/marts/mart_customer_orders.sql`, `dbt_project/models/marts/mart_customer_orders_masked.sql`, existing `schema.yml` entries, existing `exposures.yml` entry, `governance/setup_roles.sql` sections 1-4.
- New dbt package deps: none (dbt_utils `>=1.0.0,<2.0.0` already covers all tests needed).
- New Python deps: `pandas>=2.0.0,<3.0.0`, `SQLAlchemy>=2.0.0,<3.0.0` (pinned explicitly in `Dockerfile` — currently only present transitively via `mlflow`).
- No Airflow Connections/Variables — config via `os.getenv()` only, matching every existing DAG in this repo.
- No try/except in Airflow task code — guard clauses + explicit `raise` for hard failures, matching `rag_index_dag.py`'s convention.
- Postgres identifiers: literal `raw` schema (Python-created) is distinct from dbt's own `public_raw` (seed schema) and `public_olist_staging` / `public_olist_marts` (new model schemas, since no custom `generate_schema_name` macro exists in this project).
- Olist IDs are 32-char hex strings → cast `::text`, never `::int`.
- Grouping key for customer-level aggregation is `customer_unique_id`, not `customer_id` (Olist mints a new `customer_id` per order).
- CSVs live in `./data/olist/*.csv` on the host (gitignored, already set up), bind-mounted read-only to `/opt/airflow/data/olist` inside the containers.

---

### Task 1: Docker infra — pandas/SQLAlchemy pin + data bind mount

**Files:**
- Modify: `Dockerfile:31-38`
- Modify: `docker-compose.yml:39-43` (the `x-airflow-common.volumes` list)

**Interfaces:**
- Produces: `/opt/airflow/data/olist/` path visible inside `airflow-scheduler`/`airflow-webserver`/`airflow-init` containers; `pandas` and `sqlalchemy` importable in the built image without relying on `mlflow`'s transitive deps.

- [ ] **Step 1: Confirm the current gap**

Run: `docker compose exec airflow-scheduler python -c "import pandas, sqlalchemy; print('ok')"` (if the stack is already up) — this currently succeeds only because `mlflow` happens to pull both in transitively; there is no explicit pin. Also run `docker compose exec airflow-scheduler ls /opt/airflow/data` — expected: `No such file or directory` (the mount doesn't exist yet).

- [ ] **Step 2: Add explicit pins to the Dockerfile**

In `Dockerfile`, modify the existing pip install block (lines 31-38):

```dockerfile
USER airflow
RUN pip install --no-cache-dir \
        "psycopg2-binary>=2.9.6" \
        "dbt-core==1.8.0" \
        "astronomer-cosmos==1.4.3" \
        "mlflow>=2.13.0,<3.0.0" \
        "scikit-learn>=1.4.0,<2.0.0" \
        "openai>=1.30.0,<2.0.0" \
        "pandas>=2.0.0,<3.0.0" \
        "SQLAlchemy>=2.0.0,<3.0.0" \
    && pip install --no-cache-dir --no-deps "dbt-postgres==1.8.0"
```

- [ ] **Step 3: Add the data bind mount**

In `docker-compose.yml`, modify the `x-airflow-common.volumes` list (lines 39-43):

```yaml
  volumes:
    - ./dags:/opt/airflow/dags
    - .\logs:/opt/airflow/logs
    - .\plugins:/opt/airflow/plugins
    - ./dbt_project:/opt/airflow/dbt_project
    - ./data:/opt/airflow/data:ro
```

- [ ] **Step 4: Rebuild and verify**

Run: `docker compose up -d --build`
Then: `docker compose exec airflow-scheduler python -c "import pandas, sqlalchemy; print(pandas.__version__, sqlalchemy.__version__)"` — expect a clean version print, no import error.
Then: `docker compose exec airflow-scheduler ls /opt/airflow/data/olist` — expect to see `README.md` listed (proves the mount works even before the real CSVs are downloaded).

- [ ] **Step 5: Commit**

```bash
git add Dockerfile docker-compose.yml
git commit -m "Pin pandas/SQLAlchemy and mount ./data for Olist ingestion"
```

---

### Task 2: Airflow ingestion task (`ingest_olist`)

**Files:**
- Modify: `dags/dbt_pipeline_dag.py`

**Interfaces:**
- Consumes: `WAREHOUSE_DB_USER`/`WAREHOUSE_DB_PASSWORD`/`WAREHOUSE_DB_NAME` env vars (already forwarded by docker-compose); files under `/opt/airflow/data/olist/`.
- Produces: Postgres schema `raw` with 9 tables (`raw.olist_customers`, `raw.olist_orders`, `raw.olist_order_items`, `raw.olist_order_payments`, `raw.olist_order_reviews`, `raw.olist_products`, `raw.olist_sellers`, `raw.olist_geolocation`, `raw.olist_product_category_translation`) — consumed by Task 3's `sources.yml`.
- Task variable name `ingest` (the `@task`-decorated function call result) — consumed by this same task's final wiring step and no other task.

- [ ] **Step 1: Add path/mapping constants**

In `dags/dbt_pipeline_dag.py`, after the existing `DBT_EXECUTABLE` constant (after line 36), add:

```python
# ── Olist ingestion paths ──────────────────────────────────────────────────────
OLIST_DATA_DIR = Path("/opt/airflow/data/olist")
OLIST_FILES = {
    "olist_customers_dataset.csv": "olist_customers",
    "olist_orders_dataset.csv": "olist_orders",
    "olist_order_items_dataset.csv": "olist_order_items",
    "olist_order_payments_dataset.csv": "olist_order_payments",
    "olist_order_reviews_dataset.csv": "olist_order_reviews",
    "olist_products_dataset.csv": "olist_products",
    "olist_sellers_dataset.csv": "olist_sellers",
    "olist_geolocation_dataset.csv": "olist_geolocation",
    "product_category_name_translation.csv": "olist_product_category_translation",
}
```

- [ ] **Step 2: Add the `@task` import**

Modify the existing import line (line 28):
```python
from airflow.decorators import dag, task
```

- [ ] **Step 3: Add the `ingest_olist` task function**

Inside `dbt_pipeline()`, before the `# ── Step 1: resolve dbt packages ──` comment (before line 85), add:

```python
    # ── Step 0: ingest the real-world Olist dataset into the raw schema ───────
    @task
    def ingest_olist() -> dict:
        """Load the 9 Olist CSVs into Postgres schema `raw` (literal — distinct
        from dbt's own public_raw seed schema). Idempotent: each table is
        replaced in its own transaction on every run."""
        import pandas as pd
        import sqlalchemy

        missing = [f for f in OLIST_FILES if not (OLIST_DATA_DIR / f).exists()]
        if missing:
            raise FileNotFoundError(
                f"Missing Olist CSV(s) in {OLIST_DATA_DIR}: {', '.join(missing)}. "
                "Download the Kaggle 'Brazilian E-Commerce Public Dataset by "
                "Olist' (olistbr/brazilian-ecommerce) and place all 9 files "
                "there — see data/olist/README.md."
            )

        db_user = os.getenv("WAREHOUSE_DB_USER", "warehouse")
        db_password = os.getenv("WAREHOUSE_DB_PASSWORD", "warehouse")
        db_name = os.getenv("WAREHOUSE_DB_NAME", "warehouse")
        engine = sqlalchemy.create_engine(
            f"postgresql+psycopg2://{db_user}:{db_password}@postgres_warehouse:5432/{db_name}"
        )

        # Zip-code-prefix columns must stay strings or pandas drops leading
        # zeros on Brazilian CEP codes (e.g. "01046" -> 1046).
        zip_dtype_overrides = {
            "olist_customers_dataset.csv": {"customer_zip_code_prefix": str},
            "olist_sellers_dataset.csv": {"seller_zip_code_prefix": str},
            "olist_geolocation_dataset.csv": {"geolocation_zip_code_prefix": str},
        }

        with engine.begin() as conn:
            conn.execute(sqlalchemy.text("CREATE SCHEMA IF NOT EXISTS raw"))

        row_counts = {}
        for filename, table_name in OLIST_FILES.items():
            df = pd.read_csv(
                OLIST_DATA_DIR / filename,
                dtype=zip_dtype_overrides.get(filename),
            )
            with engine.begin() as conn:
                df.to_sql(
                    table_name,
                    con=conn,
                    schema="raw",
                    if_exists="replace",
                    index=False,
                    chunksize=10_000,
                    method="multi",
                )
            row_counts[table_name] = len(df)

        return row_counts

    ingest = ingest_olist()
```

- [ ] **Step 4: Wire it into the dependency chain**

Modify the existing chain (line 142):
```python
    deps >> seed >> transform >> docs
    ingest >> transform
```

- [ ] **Step 5: Update the module docstring**

Modify the pipeline-order diagram in the module docstring (lines 7-11) to:
```
Pipeline order
──────────────
  ingest_olist ──┐
  dbt_deps  ──►  dbt_seed  ──┤
                             ├──►  [dbt_transform TaskGroup]  ──►  dbt_docs_generate
                                      stg_customers       ──► mart_customer_orders
                                      stg_orders          ──►
                                      stg_olist_*         ──► mart_olist_customer_orders
                                                           ──► mart_olist_seller_performance
```

- [ ] **Step 6: Verify the fail-fast path (no CSVs downloaded yet — testable right now)**

Run: `docker compose exec airflow-scheduler airflow dags test dbt_pipeline 2024-01-01 -t ingest_olist` (or `airflow tasks test dbt_pipeline ingest_olist 2024-01-01` depending on installed Airflow CLI version)
Expected: task fails with `FileNotFoundError: Missing Olist CSV(s) in /opt/airflow/data/olist: olist_customers_dataset.csv, olist_orders_dataset.csv, ...` (all 9 listed) — confirms the guard clause works before any real data exists.

- [ ] **Step 7: Commit**

```bash
git add dags/dbt_pipeline_dag.py
git commit -m "Add ingest_olist Airflow task to load raw Olist CSVs into Postgres"
```

- [ ] **Step 8 (deferred — requires user action):** Once the user has downloaded the 9 CSVs into `./data/olist/` (see `data/olist/README.md`), re-run the same command from Step 6. Expected: task succeeds, returns a dict of row counts per table (~99k customers/orders, ~112k order_items, ~104k payments, ~99k reviews, ~33k products, ~3.1k sellers, ~1M geolocation, ~71 category translations). This step cannot be completed without the user's Kaggle download — flag it and move on to Task 3, which does not require live data.

---

### Task 3: dbt project config + sources.yml

**Files:**
- Modify: `dbt_project/dbt_project.yml`
- Create: `dbt_project/models/olist_staging/sources.yml`

**Interfaces:**
- Produces: `source('olist_raw', '<table>')` resolvable in dbt — consumed by every staging model in Task 4.

- [ ] **Step 1: Add folder-level model config**

In `dbt_project/dbt_project.yml`, modify the `models.dbt_warehouse` block (after line 29, before the `# ── Seed defaults ──` comment):

```yaml
models:
  dbt_warehouse:
    staging:
      +materialized: view      # Staging stays as views — cheap, always fresh
      +schema: staging
      +post-hook: "{{ grant_select('engineer') }}"
    marts:
      +materialized: table     # Marts are tables — fast for BI tools to query
      +schema: marts
      +post-hook: "{{ grant_select('engineer') }}"
    olist_staging:
      +materialized: view
      +schema: olist_staging
      +post-hook: "{{ grant_select('engineer') }}"
    olist_marts:
      +materialized: table
      +schema: olist_marts
      +post-hook: "{{ grant_select('engineer') }}"
```

- [ ] **Step 2: Create the sources.yml**

Create `dbt_project/models/olist_staging/sources.yml`:

```yaml
version: 2

sources:
  - name: olist_raw
    schema: raw   # literal Postgres schema created by ingest_olist — NOT dbt's
                  # own "public_raw" seed schema (see project README for the
                  # full raw / public_raw / public_olist_staging naming map).
    description: >
      Raw Olist Brazilian E-Commerce CSVs, landed by the ingest_olist Airflow
      task (dags/dbt_pipeline_dag.py) via pandas + SQLAlchemy.
    tables:
      - name: olist_customers
      - name: olist_orders
      - name: olist_order_items
      - name: olist_order_payments
      - name: olist_order_reviews
      - name: olist_products
      - name: olist_sellers
      - name: olist_geolocation
      - name: olist_product_category_translation
```

- [ ] **Step 3: Verify dbt config parses (no live data required)**

Run: `docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt_project && dbt parse --profiles-dir ."`
Expected: `Encountered an error` NOT present; parse succeeds and resolves the new `olist_raw` source (dbt parse only validates YAML/Jinja, it does not check the tables physically exist).

- [ ] **Step 4: Commit**

```bash
git add dbt_project/dbt_project.yml dbt_project/models/olist_staging/sources.yml
git commit -m "Add dbt config and sources.yml for the Olist raw tables"
```

---

### Task 4: Olist staging models (9 views)

**Files:**
- Create: `dbt_project/models/olist_staging/stg_olist_customers.sql`
- Create: `dbt_project/models/olist_staging/stg_olist_orders.sql`
- Create: `dbt_project/models/olist_staging/stg_olist_order_items.sql`
- Create: `dbt_project/models/olist_staging/stg_olist_order_payments.sql`
- Create: `dbt_project/models/olist_staging/stg_olist_order_reviews.sql`
- Create: `dbt_project/models/olist_staging/stg_olist_products.sql`
- Create: `dbt_project/models/olist_staging/stg_olist_sellers.sql`
- Create: `dbt_project/models/olist_staging/stg_olist_geolocation.sql`
- Create: `dbt_project/models/olist_staging/stg_olist_product_category_translation.sql`
- Create: `dbt_project/models/olist_staging/schema.yml`

**Interfaces:**
- Consumes: `source('olist_raw', ...)` from Task 3.
- Produces: `ref('stg_olist_customers')` (columns: `customer_id text`, `customer_unique_id text`, `customer_zip_code_prefix text`, `customer_city text`, `customer_state text`), `ref('stg_olist_orders')` (`order_id text`, `customer_id text`, `order_status text`, `order_purchase_timestamp timestamp`, `order_approved_at timestamp`, `order_delivered_carrier_date timestamp`, `order_delivered_customer_date timestamp`, `order_estimated_delivery_date timestamp`), `ref('stg_olist_order_items')` (`order_id text`, `order_item_id int`, `product_id text`, `seller_id text`, `shipping_limit_date timestamp`, `price numeric`, `freight_value numeric`), `ref('stg_olist_order_payments')` (`order_id text`, `payment_sequential int`, `payment_type text`, `payment_installments int`, `payment_value numeric`), `ref('stg_olist_order_reviews')` (`review_id text`, `order_id text`, `review_score int`, `review_comment_title text`, `review_comment_message text`, `review_creation_date timestamp`, `review_answer_timestamp timestamp`), `ref('stg_olist_products')` (`product_id text`, `product_category_name text`, `product_name_length int`, `product_description_length int`, `product_photos_qty int`, `product_weight_g numeric`, `product_length_cm numeric`, `product_height_cm numeric`, `product_width_cm numeric`), `ref('stg_olist_sellers')` (`seller_id text`, `seller_zip_code_prefix text`, `seller_city text`, `seller_state text`), `ref('stg_olist_geolocation')` (`geolocation_zip_code_prefix text`, `geolocation_lat numeric`, `geolocation_lng numeric`, `geolocation_city text`, `geolocation_state text`), `ref('stg_olist_product_category_translation')` (`product_category_name text`, `product_category_name_english text`) — all consumed by Task 5's marts.

- [ ] **Step 1: Create `stg_olist_customers.sql`**

```sql
-- stg_olist_customers: clean and type-cast the raw Olist customers table
-- Materialised as a VIEW. Note: customer_id is order-scoped (Olist mints a
-- new one per order); customer_unique_id is the real repeat-customer key.

with source as (

    select * from {{ source('olist_raw', 'olist_customers') }}

),

renamed as (

    select
        customer_id::text                as customer_id,
        customer_unique_id::text         as customer_unique_id,
        customer_zip_code_prefix::text   as customer_zip_code_prefix,
        lower(trim(customer_city))       as customer_city,
        upper(trim(customer_state))      as customer_state

    from source

)

select * from renamed
```

- [ ] **Step 2: Create `stg_olist_orders.sql`**

```sql
-- stg_olist_orders: clean and type-cast the raw Olist orders table
-- Materialised as a VIEW. No amount column exists here — order value is
-- derived downstream from stg_olist_order_payments.

with source as (

    select * from {{ source('olist_raw', 'olist_orders') }}

),

renamed as (

    select
        order_id::text                                as order_id,
        customer_id::text                              as customer_id,
        lower(trim(order_status))                      as order_status,
        order_purchase_timestamp::timestamp             as order_purchase_timestamp,
        order_approved_at::timestamp                    as order_approved_at,
        order_delivered_carrier_date::timestamp         as order_delivered_carrier_date,
        order_delivered_customer_date::timestamp        as order_delivered_customer_date,
        order_estimated_delivery_date::timestamp        as order_estimated_delivery_date

    from source

)

select * from renamed
```

- [ ] **Step 3: Create `stg_olist_order_items.sql`**

```sql
-- stg_olist_order_items: clean and type-cast the raw Olist order-items table
-- Materialised as a VIEW

with source as (

    select * from {{ source('olist_raw', 'olist_order_items') }}

),

renamed as (

    select
        order_id::text                   as order_id,
        order_item_id::int               as order_item_id,
        product_id::text                 as product_id,
        seller_id::text                  as seller_id,
        shipping_limit_date::timestamp   as shipping_limit_date,
        price::numeric(10, 2)            as price,
        freight_value::numeric(10, 2)    as freight_value

    from source

)

select * from renamed
```

- [ ] **Step 4: Create `stg_olist_order_payments.sql`**

```sql
-- stg_olist_order_payments: clean and type-cast the raw Olist payments table
-- Materialised as a VIEW

with source as (

    select * from {{ source('olist_raw', 'olist_order_payments') }}

),

renamed as (

    select
        order_id::text                    as order_id,
        payment_sequential::int           as payment_sequential,
        lower(trim(payment_type))         as payment_type,
        payment_installments::int         as payment_installments,
        payment_value::numeric(10, 2)     as payment_value

    from source

)

select * from renamed
```

- [ ] **Step 5: Create `stg_olist_order_reviews.sql`**

```sql
-- stg_olist_order_reviews: clean and type-cast the raw Olist reviews table
-- Materialised as a VIEW

with source as (

    select * from {{ source('olist_raw', 'olist_order_reviews') }}

),

renamed as (

    select
        review_id::text                          as review_id,
        order_id::text                            as order_id,
        review_score::int                         as review_score,
        trim(review_comment_title)                as review_comment_title,
        trim(review_comment_message)              as review_comment_message,
        review_creation_date::timestamp           as review_creation_date,
        review_answer_timestamp::timestamp        as review_answer_timestamp

    from source

)

select * from renamed
```

- [ ] **Step 6: Create `stg_olist_products.sql`**

```sql
-- stg_olist_products: clean and type-cast the raw Olist products table
-- Materialised as a VIEW. Renames the upstream "lenght" column-name typos.

with source as (

    select * from {{ source('olist_raw', 'olist_products') }}

),

renamed as (

    select
        product_id::text                            as product_id,
        trim(product_category_name)                 as product_category_name,
        product_name_lenght::int                    as product_name_length,
        product_description_lenght::int             as product_description_length,
        product_photos_qty::int                      as product_photos_qty,
        product_weight_g::numeric                    as product_weight_g,
        product_length_cm::numeric                   as product_length_cm,
        product_height_cm::numeric                   as product_height_cm,
        product_width_cm::numeric                    as product_width_cm

    from source

)

select * from renamed
```

- [ ] **Step 7: Create `stg_olist_sellers.sql`**

```sql
-- stg_olist_sellers: clean and type-cast the raw Olist sellers table
-- Materialised as a VIEW

with source as (

    select * from {{ source('olist_raw', 'olist_sellers') }}

),

renamed as (

    select
        seller_id::text                    as seller_id,
        seller_zip_code_prefix::text       as seller_zip_code_prefix,
        lower(trim(seller_city))          as seller_city,
        upper(trim(seller_state))         as seller_state

    from source

)

select * from renamed
```

- [ ] **Step 8: Create `stg_olist_geolocation.sql`**

```sql
-- stg_olist_geolocation: DEVIATION from the thin 1:1 staging convention used
-- elsewhere. The raw table has ~1M rows with many duplicate/near-duplicate
-- lat/lng rows per zip prefix — not a usable grain as-is, so this model
-- aggregates to one row per zip-code prefix before anything downstream joins to it.

with source as (

    select * from {{ source('olist_raw', 'olist_geolocation') }}

),

renamed as (

    select
        geolocation_zip_code_prefix::text   as geolocation_zip_code_prefix,
        geolocation_lat::numeric            as geolocation_lat,
        geolocation_lng::numeric            as geolocation_lng,
        lower(trim(geolocation_city))       as geolocation_city,
        upper(trim(geolocation_state))      as geolocation_state

    from source

),

aggregated as (

    select
        geolocation_zip_code_prefix,
        avg(geolocation_lat)                                as geolocation_lat,
        avg(geolocation_lng)                                as geolocation_lng,
        mode() within group (order by geolocation_city)     as geolocation_city,
        mode() within group (order by geolocation_state)    as geolocation_state

    from renamed
    group by geolocation_zip_code_prefix

)

select * from aggregated
```

- [ ] **Step 9: Create `stg_olist_product_category_translation.sql`**

```sql
-- stg_olist_product_category_translation: clean and type-cast the raw
-- category-translation table. Materialised as a VIEW.

with source as (

    select * from {{ source('olist_raw', 'olist_product_category_translation') }}

),

renamed as (

    select
        trim(product_category_name)             as product_category_name,
        trim(product_category_name_english)      as product_category_name_english

    from source

)

select * from renamed
```

- [ ] **Step 10: Create `schema.yml`**

```yaml
version: 2

models:
  - name: stg_olist_customers
    description: "Cleaned and type-cast Olist customer records. customer_id is order-scoped; customer_unique_id is the real repeat-customer key."
    meta:
      owner: Ellinei
      team: Data Engineering
      tier: Tier 2
    tags: [staging, olist, customers]
    columns:
      - name: customer_id
        description: "Order-scoped customer identifier (Olist mints a new one per order)."
        tests: [not_null, unique]
      - name: customer_unique_id
        description: "The real repeat-customer identifier — use this for customer-level grouping."
        tests: [not_null]

  - name: stg_olist_orders
    description: "Cleaned and type-cast Olist order records."
    meta:
      owner: Ellinei
      team: Data Engineering
      tier: Tier 2
    tags: [staging, olist, orders]
    columns:
      - name: order_id
        description: "Unique identifier for each order."
        tests: [not_null, unique]
      - name: customer_id
        description: "Foreign key referencing stg_olist_customers.customer_id."
        tests:
          - not_null
          - relationships:
              to: ref('stg_olist_customers')
              field: customer_id
      - name: order_status
        description: "Current order status (lowercased)."
        tests:
          - not_null
          - accepted_values:
              values: ["created", "invoiced", "processing", "unavailable", "shipped", "delivered", "canceled", "approved"]

  - name: stg_olist_order_items
    description: "Cleaned and type-cast Olist order line items — one row per item within an order."
    meta:
      owner: Ellinei
      team: Data Engineering
      tier: Tier 2
    tags: [staging, olist, orders]
    columns:
      - name: order_id
        description: "Foreign key referencing stg_olist_orders.order_id."
        tests:
          - not_null
          - relationships:
              to: ref('stg_olist_orders')
              field: order_id
      - name: seller_id
        description: "Foreign key referencing stg_olist_sellers.seller_id."
        tests:
          - not_null
          - relationships:
              to: ref('stg_olist_sellers')
              field: seller_id
      - name: price
        description: "Item price."
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"

  - name: stg_olist_order_payments
    description: "Cleaned and type-cast Olist payment records — one row per payment installment/method per order."
    meta:
      owner: Ellinei
      team: Data Engineering
      tier: Tier 2
    tags: [staging, olist, orders]
    columns:
      - name: order_id
        description: "Foreign key referencing stg_olist_orders.order_id."
        tests:
          - not_null
          - relationships:
              to: ref('stg_olist_orders')
              field: order_id
      - name: payment_type
        description: "Payment method (lowercased)."
        tests:
          - not_null
          - accepted_values:
              values: ["credit_card", "boleto", "voucher", "debit_card", "not_defined"]
      - name: payment_value
        description: "Payment amount."
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"

  - name: stg_olist_order_reviews
    description: "Cleaned and type-cast Olist customer review records."
    meta:
      owner: Ellinei
      team: Data Engineering
      tier: Tier 2
    tags: [staging, olist, reviews]
    columns:
      - name: review_id
        description: "Unique identifier for each review."
        tests: [not_null]
      - name: order_id
        description: "Foreign key referencing stg_olist_orders.order_id."
        tests:
          - not_null
          - relationships:
              to: ref('stg_olist_orders')
              field: order_id
      - name: review_score
        description: "Customer review score, 1-5."
        tests:
          - not_null
          - accepted_values:
              values: [1, 2, 3, 4, 5]

  - name: stg_olist_products
    description: "Cleaned and type-cast Olist product records (fixes upstream 'lenght' column-name typos)."
    meta:
      owner: Ellinei
      team: Data Engineering
      tier: Tier 2
    tags: [staging, olist, products]
    columns:
      - name: product_id
        description: "Unique identifier for each product."
        tests: [not_null, unique]

  - name: stg_olist_sellers
    description: "Cleaned and type-cast Olist seller records."
    meta:
      owner: Ellinei
      team: Data Engineering
      tier: Tier 2
    tags: [staging, olist, sellers]
    columns:
      - name: seller_id
        description: "Unique identifier for each seller."
        tests: [not_null, unique]

  - name: stg_olist_geolocation
    description: >
      Olist zip-code-prefix geolocation, aggregated to one row per prefix
      (raw table has ~1M rows with duplicate lat/lng per prefix).
    meta:
      owner: Ellinei
      team: Data Engineering
      tier: Tier 3
    tags: [staging, olist, geolocation]
    columns:
      - name: geolocation_zip_code_prefix
        description: "Zip-code prefix — one row per prefix after aggregation."
        tests: [not_null, unique]

  - name: stg_olist_product_category_translation
    description: "Maps Olist's Portuguese product category names to English."
    meta:
      owner: Ellinei
      team: Data Engineering
      tier: Tier 3
    tags: [staging, olist, products]
    columns:
      - name: product_category_name
        description: "Portuguese product category name."
        tests: [not_null, unique]
```

- [ ] **Step 11: Verify compilation (no live data required)**

Run: `docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt_project && dbt compile --select path:models/olist_staging --profiles-dir ."`
Expected: all 9 models compile with no Jinja/SQL syntax errors (compilation does not require the `raw.*` tables to physically exist yet).

- [ ] **Step 12: Commit**

```bash
git add dbt_project/models/olist_staging/
git commit -m "Add 9 Olist staging models with schema tests"
```

- [ ] **Step 13 (deferred — requires Task 2 Step 8 to have run against real data):** `dbt run --select path:models/olist_staging` then `dbt test --select path:models/olist_staging` inside the scheduler container. Expected: all 9 views build; watch specifically for `accepted_values` failures on `order_status`/`payment_type` if the actual CSV contains a value not in the lists above — the fix is adding the observed value, not a design flaw.

---

### Task 5: Olist marts (2 tables) + exposure

**Files:**
- Create: `dbt_project/models/olist_marts/mart_olist_customer_orders.sql`
- Create: `dbt_project/models/olist_marts/mart_olist_seller_performance.sql`
- Create: `dbt_project/models/olist_marts/schema.yml`
- Modify: `dbt_project/models/marts/exposures.yml` (append only — existing entry untouched)

**Interfaces:**
- Consumes: `ref('stg_olist_customers')`, `ref('stg_olist_orders')`, `ref('stg_olist_order_payments')`, `ref('stg_olist_sellers')`, `ref('stg_olist_order_items')`, `ref('stg_olist_order_reviews')` from Task 4.
- Produces: `mart_olist_customer_orders` (`customer_unique_id` PK, `total_orders`, `lifetime_value`, `avg_order_value`, `first_order_date`, `last_order_date`, `delivered_orders`, `cancelled_orders`), `mart_olist_seller_performance` (`seller_id` PK, `seller_city`, `seller_state`, `total_orders`, `total_revenue`, `total_freight`, `avg_review_score`, `reviewed_orders`) — consumed by Task 6's RLS policies and BI/exposure.

- [ ] **Step 1: Create `mart_olist_customer_orders.sql`**

```sql
-- mart_olist_customer_orders: one row per unique Olist customer with
-- aggregated order metrics. Grouped by customer_unique_id — NOT customer_id,
-- since Olist mints a new customer_id per order (grouping by customer_id
-- would make every "customer" trivially show exactly 1 order).

with orders as (

    select * from {{ ref('stg_olist_orders') }}

),

customers as (

    select * from {{ ref('stg_olist_customers') }}

),

order_amounts as (

    select
        order_id,
        sum(payment_value) as amount

    from {{ ref('stg_olist_order_payments') }}
    group by order_id

),

orders_with_amount as (

    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,
        coalesce(a.amount, 0.00) as amount

    from orders o
    left join order_amounts a using (order_id)

),

customer_orders as (

    select
        c.customer_unique_id,
        o.order_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.amount

    from orders_with_amount o
    inner join customers c using (customer_id)

),

order_summary as (

    select
        customer_unique_id,
        count(order_id)                                            as total_orders,
        sum(amount)                                                as lifetime_value,
        avg(amount)                                                as avg_order_value,
        min(order_purchase_timestamp)                              as first_order_date,
        max(order_purchase_timestamp)                              as last_order_date,
        count(order_id) filter (where order_status = 'delivered')  as delivered_orders,
        count(order_id) filter (where order_status = 'canceled')   as cancelled_orders

    from customer_orders
    group by customer_unique_id

)

select * from order_summary
```

- [ ] **Step 2: Create `mart_olist_seller_performance.sql`**

```sql
-- mart_olist_seller_performance: one row per seller with revenue, freight,
-- and review-score aggregates across all their order items.

with sellers as (

    select * from {{ ref('stg_olist_sellers') }}

),

order_items as (

    select * from {{ ref('stg_olist_order_items') }}

),

reviews as (

    select * from {{ ref('stg_olist_order_reviews') }}

),

item_reviews as (

    select
        oi.seller_id,
        oi.order_id,
        oi.price,
        oi.freight_value,
        r.review_score

    from order_items oi
    left join reviews r using (order_id)

),

seller_summary as (

    select
        seller_id,
        count(distinct order_id) as total_orders,
        sum(price)               as total_revenue,
        sum(freight_value)       as total_freight,
        avg(review_score)        as avg_review_score,
        count(review_score)      as reviewed_orders

    from item_reviews
    group by seller_id

)

select
    s.seller_id,
    s.seller_city,
    s.seller_state,
    coalesce(ss.total_orders, 0)      as total_orders,
    coalesce(ss.total_revenue, 0.00)  as total_revenue,
    coalesce(ss.total_freight, 0.00)  as total_freight,
    ss.avg_review_score,
    coalesce(ss.reviewed_orders, 0)   as reviewed_orders

from sellers s
left join seller_summary ss using (seller_id)
```

- [ ] **Step 3: Create `schema.yml`**

```yaml
version: 2

models:
  - name: mart_olist_customer_orders
    description: >
      One row per unique Olist customer (grouped by customer_unique_id, not
      the order-scoped customer_id) enriched with aggregated order metrics.
    meta:
      owner: Ellinei
      team: Analytics
      tier: Tier 2
    tags: [mart, olist, customers, BI-ready]
    columns:
      - name: customer_unique_id
        description: "Primary key — unique per real-world customer."
        tests: [not_null, unique]
      - name: lifetime_value
        description: "Sum of payment amounts across the customer's order history."
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: total_orders
        description: "Total number of orders placed by the customer."
        tests: [not_null]

  - name: mart_olist_seller_performance
    description: >
      One row per Olist seller with revenue, freight, and review-score
      aggregates across all their order items.
    meta:
      owner: Ellinei
      team: Analytics
      tier: Tier 2
    tags: [mart, olist, sellers, BI-ready]
    columns:
      - name: seller_id
        description: "Primary key — unique per seller."
        tests: [not_null, unique]
      - name: total_revenue
        description: "Sum of item prices sold by this seller (excludes freight)."
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: avg_review_score
        description: "Average customer review score (1-5) across the seller's order items."
```

- [ ] **Step 4: Append to `exposures.yml`**

In `dbt_project/models/marts/exposures.yml`, append (do not touch the existing `customer_orders_dashboard` entry):

```yaml
  - name: olist_ecommerce_dashboard
    type: dashboard
    maturity: low
    description: >
      Olist e-commerce dashboard — customer lifetime value and seller
      performance at real (~100k order) scale.
    depends_on:
      - ref('mart_olist_customer_orders')
      - ref('mart_olist_seller_performance')
    owner:
      name: Ellinei
      email: nonamex01@outlook.com
```

- [ ] **Step 5: Verify compilation (no live data required)**

Run: `docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt_project && dbt compile --select path:models/olist_marts --profiles-dir ."`
Expected: both marts compile with no syntax errors.

- [ ] **Step 6: Commit**

```bash
git add dbt_project/models/olist_marts/ dbt_project/models/marts/exposures.yml
git commit -m "Add Olist customer-orders and seller-performance marts"
```

- [ ] **Step 7 (deferred — requires Task 4 Step 13 to have run against real data):** `dbt run --select path:models/olist_marts` then `dbt test --select path:models/olist_marts`. Then query `mart_olist_customer_orders` and confirm `total_orders` is not trivially 1 for every row (validates the `customer_unique_id` grouping decision empirically). Then query `mart_olist_seller_performance` for sane aggregates (non-zero revenue, `avg_review_score` between 1 and 5).

---

### Task 6: Governance grants + RLS for the new schemas

**Files:**
- Modify: `governance/setup_roles.sql` (append a new section 5 — sections 1-4 untouched)

**Interfaces:**
- Consumes: schemas `raw`, `public_olist_staging`, `public_olist_marts` and tables `mart_olist_customer_orders`/`mart_olist_seller_performance` — these must physically exist (i.e. Task 4/5's deferred live-data steps must have run) before this SQL can execute successfully, since `GRANT ... ON SCHEMA X` fails if schema `X` doesn't exist yet.

- [ ] **Step 1: Append the new governance section**

In `governance/setup_roles.sql`, append after line 77 (the end of section 4):

```sql

-- ── 5. Olist schemas — grants + RLS ────────────────────────────────────────────
-- Olist's customer table carries no name/email (only IDs, zip prefix, city,
-- state) — materially less PII pressure than the toy demo's mart, so analyst
-- gets a direct grant on both new marts instead of a masked view.

GRANT USAGE ON SCHEMA raw                  TO engineer;
GRANT USAGE ON SCHEMA public_olist_staging TO engineer;
GRANT USAGE ON SCHEMA public_olist_marts   TO engineer;
GRANT USAGE ON SCHEMA public_olist_marts   TO analyst;

GRANT SELECT ON ALL TABLES IN SCHEMA raw                  TO engineer;
GRANT SELECT ON ALL TABLES IN SCHEMA public_olist_staging TO engineer;
GRANT SELECT ON ALL TABLES IN SCHEMA public_olist_marts   TO engineer;

ALTER DEFAULT PRIVILEGES IN SCHEMA raw                  GRANT SELECT ON TABLES TO engineer;
ALTER DEFAULT PRIVILEGES IN SCHEMA public_olist_staging GRANT SELECT ON TABLES TO engineer;
ALTER DEFAULT PRIVILEGES IN SCHEMA public_olist_marts   GRANT SELECT ON TABLES TO engineer;

GRANT SELECT ON public_olist_marts.mart_olist_customer_orders   TO analyst;
GRANT SELECT ON public_olist_marts.mart_olist_seller_performance TO analyst;

ALTER TABLE public_olist_marts.mart_olist_customer_orders ENABLE ROW LEVEL SECURITY;

DO $$ BEGIN
  CREATE POLICY engineer_full_access
    ON public_olist_marts.mart_olist_customer_orders
    FOR SELECT TO engineer
    USING (true);
EXCEPTION WHEN duplicate_object THEN RAISE NOTICE 'engineer_full_access already exists';
END $$;

DO $$ BEGIN
  CREATE POLICY analyst_delivered_only
    ON public_olist_marts.mart_olist_customer_orders
    FOR SELECT TO analyst
    USING (delivered_orders > 0);
EXCEPTION WHEN duplicate_object THEN RAISE NOTICE 'analyst_delivered_only already exists';
END $$;
```

- [ ] **Step 2 (deferred — requires Task 5 Step 7 to have run first, so `public_olist_marts` exists):** Apply it:

Run: `docker compose exec -T postgres_warehouse psql -U warehouse -d warehouse < governance/setup_roles.sql`
Expected: no errors; `NOTICE` lines are fine (they mean a role/policy already existed from a prior run — this file is idempotent).

- [ ] **Step 3 (deferred, same prerequisite): Verify access as each role**

```bash
docker compose exec postgres_warehouse psql -U engineer_user -d warehouse -c "select count(*) from public_olist_marts.mart_olist_customer_orders;"
docker compose exec postgres_warehouse psql -U analyst_user -d warehouse -c "select count(*) from public_olist_marts.mart_olist_customer_orders;"
```
Expected: `engineer_user` sees the full row count; `analyst_user` sees only rows where `delivered_orders > 0` (a smaller or equal count).

- [ ] **Step 4: Commit**

```bash
git add governance/setup_roles.sql
git commit -m "Extend governance roles/RLS to cover the new Olist schemas"
```

---

### Task 7: Full end-to-end verification (requires the user's Kaggle download)

This task has no code changes — it's the gate that confirms Tasks 1-6 work together against real data. **Blocked until the user downloads the 9 CSVs into `./data/olist/`** (Kaggle requires authentication, which the implementing agent cannot do on the user's behalf).

- [ ] **Step 1:** Confirm all 9 files are present: `ls data/olist/*.csv` should list exactly 9 files matching the names in `data/olist/README.md`.
- [ ] **Step 2:** Trigger the full `dbt_pipeline` DAG (UI or `airflow dags trigger dbt_pipeline`).
- [ ] **Step 3:** In the Airflow graph view, confirm `ingest_olist` succeeded and the `dbt_transform` TaskGroup now shows the 9 new `stg_olist_*` and 2 `mart_olist_*` nodes alongside the untouched toy-demo nodes.
- [ ] **Step 4:** Run Task 2 Step 8, Task 4 Step 13, Task 5 Step 7, and Task 6 Steps 2-3 (all previously deferred) now that real data exists.
- [ ] **Step 5:** `dbt docs generate` and confirm the source→staging→marts lineage renders for the new `olist_raw` source (first time this project has source-based lineage).
- [ ] **Step 6:** Re-trigger the DAG a second time. Confirm `raw.olist_*` row counts stay stable (no doubling) and all dbt tests still pass — validates idempotency end-to-end.
- [ ] **Step 7 (optional):** Temporarily rename one CSV, re-trigger, confirm `ingest_olist` fails fast with the explicit missing-file error and (if `SLACK_WEBHOOK_URL` is set in `.env`) a Slack message arrives — validates the failure-handling design empirically. Rename the file back afterward.
