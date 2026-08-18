# Airflow + dbt + PostgreSQL — Local Data Pipeline

A portfolio-grade data engineering project that wires together **Apache Airflow 2.9**, **dbt-core 1.8**, and **PostgreSQL 15** entirely inside Docker.

Two pipelines run side by side on the same infrastructure:

1. **Toy demo** (below) — a tiny, always-works showcase of the Airflow → dbt → Postgres wiring itself (10-15 rows).
2. **[Real-world Olist e-commerce pipeline](#real-world-data-pipeline-olist)** — the same wiring proven against ~100k real, anonymized orders, with its own ingestion task, dbt models, governance, and MLOps training run.

---

## Architecture

```
┌───────────────────────────────────────────────────────────────┐
│  Docker network                                               │
│                                                               │
│  ┌─────────────────┐     metadata      ┌──────────────────┐   │
│  │ Airflow         │ ◄──────────────►  │ postgres_airflow │   │
│  │  · webserver    │    (port 5432)    │ (Airflow DB)     │   │
│  │  · scheduler    │                   └──────────────────┘   │
│  │                 │                                          │
│  │  [dbt_pipeline] │   dbt run/test   ┌───────────────────┐   │
│  │  · dbt_seed     │ ──────────────►  │ postgres_warehouse│   │
│  │  · DbtTaskGroup │                  │ (port 5433 host)  │   │
│  │    stg_customers│                  │  schema: raw      │   │
│  │    stg_orders   │                  │  schema: staging  │   │
│  │    mart_customer│                  │  schema: marts    │   │
│  │    _orders      │                  └───────────────────┘   │
│  └─────────────────┘                                          │
└───────────────────────────────────────────────────────────────┘
```

| Service               | Purpose                                                              | Dev port | Prod port | Env var |
|-----------------------|-----------------------------------------------------------------------|----------|-----------|---------|
| `airflow-webserver`   | Airflow UI                                                           | 8080     | 8090      | `AIRFLOW_WEBSERVER_PORT` |
| `airflow-scheduler`   | DAG scheduling                                                       | —        | —         | — |
| `airflow-init`        | One-shot DB migration + admin user                                   | —        | —         | — |
| `postgres_airflow`    | Airflow metadata store                                               | 5432     | 5442      | `AIRFLOW_DB_PORT` |
| `postgres_warehouse`  | dbt target / data warehouse                                          | 5433     | 5443      | `WAREHOUSE_DB_PORT` |
| `mlflow-server`       | MLflow tracking UI + artifact store (`--profile mlops` in dev; always-on in prod) | 5000     | 5010      | `MLFLOW_PORT` |

---

## Running dev and prod side by side

A second "prod" stack can run at the same time as dev, on the same machine, using the same
`docker-compose.yml` — every difference between them is an env var, not a second compose file.

```bash
# Dev (default — unchanged, no -p flag, keeps existing containers/volumes)
docker compose up -d

# Prod (side by side — separate project name + separate env file)
docker compose -p airflow-dbt-prod --env-file .env.prod up -d
```

`mlflow-server` auto-starts in the prod stack via `COMPOSE_PROFILES=prod-core` in `.env.prod` — no
extra `--profile` flag needed on the command line (dev still requires `--profile mlops` to start it
manually).

> **Shared between both stacks:** host directories (`./dags`, `./logs`, `./dbt_project`, `./data`)
> are mounted into *both* stacks, and the Docker image tag they build/run from is also shared —
> containers, volumes, and ports are the only things isolated per project. Concretely:
> - **`./logs`** is written by both stacks' schedulers unconditionally, from the moment both stacks
>   are up — no DAG needs to run. Both append to
>   `logs/dag_processor_manager/dag_processor_manager.log` and write into
>   `logs/scheduler/<date>/` continuously. Because both stacks also share `./dags` (identical DAG
>   definitions) and both run `@daily`, an unpaused DAG in both stacks produces the *same* `run_id`,
>   so task logs land at the *same* path (`logs/dag_id=…/run_id=scheduled__…/task_id=…/attempt=1.log`)
>   — prod's UI could end up serving a file dev also wrote.
> - **`dbt_project/target/`/`dbt_packages/`** race if `dbt_pipeline` is triggered in both stacks at
>   the same moment — avoid running it in both at once.
> - **The image tag `airflow-dbt-pipeline:latest`** (`x-airflow-common.image` in
>   `docker-compose.yml`) is daemon-global, not project-scoped — rebuilding from either stack retags
>   what the other picks up on its next container recreate. This never actually breaks anything
>   (identical Dockerfile, shared `./dags` guarantees identical code either way) but it is a shared
>   resource, not an isolated one.
>
> **Container renaming:** this phase also removed `container_name:` from dev's core services, so the
> next `docker compose up -d` you run recreates dev's containers under Compose's auto-generated
> names (e.g. `postgres_airflow` → `airflowdbtpipeline-postgres_airflow-1`). Data is unaffected —
> named volumes are unchanged, only container identity changes — but any personal script or alias
> that calls `docker exec -it postgres_warehouse ...` (or similar literal names) will break with "No
> such container" until updated to `docker compose exec <service-name>` (see "Run dbt manually
> inside the scheduler container" below).

---

## Data Flow

```
seeds/raw_customers.csv  ──┐
                            ├─► dbt seed ──► raw.raw_customers
seeds/raw_orders.csv     ──┘              ► raw.raw_orders
                                                │
                                    ┌───────────┴───────────┐
                                    ▼                       ▼
                             stg_customers           stg_orders
                             (staging schema,        (staging schema,
                              VIEW)                   VIEW)
                                    │                       │
                                    └───────────┬───────────┘
                                                ▼
                                     mart_customer_orders
                                     (marts schema, TABLE)
```

### Models

| Layer    | Model                  | Materialisation | Description                               |
|----------|------------------------|-----------------|-------------------------------------------|
| Seeds    | `raw_customers`        | table           | 10 sample customers loaded from CSV       |
| Seeds    | `raw_orders`           | table           | 15 sample orders loaded from CSV          |
| Staging  | `stg_customers`        | view            | Type-cast + normalised customer records   |
| Staging  | `stg_orders`           | view            | Type-cast + normalised order records      |
| Marts    | `mart_customer_orders` | table           | Per-customer order aggregates for BI      |

### Tests

dbt schema tests are defined in each layer's `schema.yml`:

- `not_null` and `unique` on all primary keys
- `unique` on email addresses
- `relationships` test ensuring every order's `customer_id` exists in `stg_customers`
- `accepted_values` on `stg_orders.status`

---

## Real-World Data Pipeline (Olist)

Proves the same Airflow + dbt + Postgres wiring at real scale, using the Kaggle **"Brazilian E-Commerce Public Dataset by Olist"** (`olistbr/brazilian-ecommerce`) — ~100k real, anonymized orders across 9 raw tables. Runs entirely alongside the toy demo above; nothing in it was modified.

```
data/olist/*.csv (user-downloaded, gitignored)
        │  bind-mounted read-only: ./data → /opt/airflow/data
        ▼
Airflow task `ingest_olist` (pandas + SQLAlchemy)
        │  loads into literal Postgres schema `raw` (raw.olist_*)
        ▼
dbt source('olist_raw', ...)
        │
   models/olist_staging/*.sql  (9 views, schema public_olist_staging)
        │
   models/olist_marts/*.sql    (2 tables, schema public_olist_marts)
        │
   ├─► mart_olist_customer_orders   (one row per customer, grouped by
        │                            customer_unique_id — Olist mints a
        │                            new customer_id per order)
        └─► mart_olist_seller_performance (one row per seller: revenue,
                                            freight, review scores)
```

### Models

| Layer   | Model                              | Materialisation | Description |
|---------|-------------------------------------|-----------------|--------------|
| Staging | `stg_olist_customers`               | view | Customer records (order-scoped `customer_id` + the real repeat-customer `customer_unique_id`) |
| Staging | `stg_olist_orders`                  | view | Order records, status + timestamps |
| Staging | `stg_olist_order_items`             | view | Line items — product, seller, price, freight |
| Staging | `stg_olist_order_payments`          | view | Payment method/value per order |
| Staging | `stg_olist_order_reviews`           | view | Customer review scores + comments |
| Staging | `stg_olist_products`                | view | Product catalog (fixes upstream column-name typos) |
| Staging | `stg_olist_sellers`                 | view | Seller records |
| Staging | `stg_olist_geolocation`             | view | Zip-code geolocation, aggregated to one row per prefix (raw table has ~1M near-duplicate rows) |
| Staging | `stg_olist_product_category_translation` | view | Portuguese → English category names |
| Marts   | `mart_olist_customer_orders`        | table | Per-customer lifetime value, order counts, delivered/cancelled breakdown |
| Marts   | `mart_olist_seller_performance`     | table | Per-seller revenue, freight, average review score |

43 dbt tests cover primary keys, foreign-key relationships, and value ranges/vocabularies (`order_status`, `payment_type`, `review_score`).

### Governance

The same `engineer`/`analyst` role split from the toy demo (`governance/setup_roles.sql`) extends to the new schemas: `engineer` has full read access; `analyst` gets a direct grant on both marts (Olist's customer table carries no name/email, so no masking is needed) plus row-level security on `mart_olist_customer_orders` restricting analysts to delivered orders only.

### MLOps

`mlflow_training_olist` DAG (weekly, `--profile mlops`) trains a `RandomForestRegressor` on `mart_olist_customer_orders` to predict customer lifetime value — the same pattern as the toy demo's `mlflow_training` DAG, but against real signal instead of ~10 rows.

### Setup

1. Download the dataset from Kaggle (`olistbr/brazilian-ecommerce`) — see `data/olist/README.md` for the exact 9 filenames expected.
2. Place all 9 CSVs directly in `./data/olist/` (gitignored — never commit them).
3. `docker compose up -d --build`, then trigger `dbt_pipeline` — `ingest_olist` loads the raw tables, then Cosmos auto-discovers and runs the new dbt models alongside the toy demo's.

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (with Compose v2)
- ~4 GB RAM allocated to Docker

---

## Secrets & Configuration

Copy `.env.example` to `.env` and fill in real values before starting the stack — `.env` is
gitignored and never committed. `.env.example` documents every variable, including how to
generate a fresh Airflow Fernet key.

> Older commits in this repo's history contain local-dev-only demo credentials (Airflow Fernet
> key, default admin password, OpenMetadata service passwords) that predate this file. They have
> since been rotated; current credentials live only in your own `.env`.

For the optional prod stack, copy `.env.prod.example` to `.env.prod` and fill in **independently
generated** secrets — never copy values from `.env`. `.env.prod` is gitignored the same way `.env`
is.

If you use the optional `catalog` profile (OpenMetadata), the ingestion CLI configs are
generated from tracked templates rather than committed directly:
```bash
envsubst < openmetadata/postgres_ingestion.yaml.template > openmetadata/postgres_ingestion.yaml
envsubst < openmetadata/dbt_ingestion.yaml.template > openmetadata/dbt_ingestion.yaml
```

---

## Quick Start

```bash
# 1. Clone / open the project folder, then:
cd "Airflow + dbt pipeline"

# 2. Start all services
#    First run installs dbt + Cosmos (~2-3 min due to _PIP_ADDITIONAL_REQUIREMENTS)
docker compose up -d

# 3. Watch the init container finish before opening the UI
docker compose logs -f airflow-init

# 4. Open Airflow UI
#    http://localhost:8080   login: admin / $AIRFLOW_ADMIN_PASSWORD (see .env)

# 5. Trigger the DAG manually from the UI, or wait for the daily schedule
```

### Prod stack (optional, runs alongside dev)

```bash
# 1. Copy the prod template and fill in independently-generated secrets
cp .env.prod.example .env.prod

# 2. Start the prod stack under its own Compose project name
docker compose -p airflow-dbt-prod --env-file .env.prod up -d

# 3. Open the prod Airflow UI
#    http://localhost:8090   login: admin / $AIRFLOW_ADMIN_PASSWORD (see .env.prod)
```

See "Running dev and prod side by side" above for the shared-resource and container-renaming caveats.

### Run dbt manually inside the scheduler container

```bash
docker compose exec airflow-scheduler bash
# Prod stack equivalent:
docker compose -p airflow-dbt-prod --env-file .env.prod exec airflow-scheduler bash
```

Inside the container:
```bash
cd /opt/airflow/dbt_project
dbt seed   --profiles-dir .
dbt run    --profiles-dir .
dbt test   --profiles-dir .
dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir . --port 8081
```

### Connect to the warehouse directly

```
Host:     localhost
Port:     5433
Database: $WAREHOUSE_DB_NAME  (see .env — default "warehouse")
User:     $WAREHOUSE_DB_USER  (see .env — default "warehouse")
Password: $WAREHOUSE_DB_PASSWORD  (see .env)
```

(Prod stack: port 5443, see `.env.prod`.)

---

## Project Structure

```
.
├── .env                          # Credentials (never commit to git)
├── .env.example                  # Template listing every required variable
├── .env.prod                     # Prod stack credentials (never commit to git)
├── .env.prod.example             # Template listing every required prod variable
├── docker-compose.yml            # All services
├── Dockerfile                    # Custom Airflow image (dbt, Cosmos, pandas, mlflow...)
├── dags/
│   ├── dbt_pipeline_dag.py       # Main DAG: ingest_olist + dbt seed/run/test (Cosmos)
│   ├── mlflow_training_dag.py    # Toy-demo MLOps training run
│   ├── mlflow_training_olist_dag.py  # Real-data MLOps training run
│   └── rag_index_dag.py          # Embeds dbt catalog descriptions into pgvector
├── data/
│   └── olist/                    # Real Olist CSVs (gitignored — see README.md there)
├── dbt_project/
│   ├── dbt_project.yml           # dbt project config
│   ├── profiles.yml              # Warehouse connection (reads env vars)
│   ├── seeds/
│   │   ├── raw_customers.csv
│   │   └── raw_orders.csv
│   ├── models/
│   │   ├── staging/              # Toy demo staging (stg_customers, stg_orders)
│   │   ├── marts/                # Toy demo marts + Power BI exposures
│   │   ├── olist_staging/        # 9 Olist staging views + sources.yml
│   │   └── olist_marts/          # 2 Olist marts (customer orders, seller performance)
│   ├── macros/
│   │   └── grant_select.sql      # Post-hook: grants + concurrency-safe schema lock
│   └── tests/                    # Custom singular tests
├── governance/
│   └── setup_roles.sql           # Roles, schema grants, row-level security
├── docs/superpowers/              # Design specs + implementation plans
├── logs/                         # Airflow task logs (auto-populated)
└── plugins/                      # Custom Airflow plugins (empty)
```

---

## Key Design Decisions

| Decision | Rationale |
|---|---|
| `_PIP_ADDITIONAL_REQUIREMENTS` | Simple for local dev; no custom Dockerfile needed. In production, pre-bake packages into a `Dockerfile` to avoid 2-3 min install on every container restart. |
| Two PostgreSQL containers | Mirrors real-world separation of Airflow metadata and the analytics warehouse — same pattern as Snowflake + managed Airflow in production. |
| Cosmos `LoadMode.DBT_LS` | Cosmos calls `dbt ls` at scheduler startup to auto-discover all models. You get one Airflow task per node without hard-coding anything. |
| File-based `profiles.yml` | Avoids needing an Airflow Connection object for the warehouse — credentials flow from `.env` → docker-compose env vars → dbt `env_var()`. |
| Staging as VIEWs, Marts as TABLEs | Standard dbt layering: staging is cheap to rebuild, marts are materialised for BI performance. |
| One shared `docker-compose.yml` for dev + prod | Every dev/prod difference is a scalar env-var value (ports, `DBT_TARGET`, `COMPOSE_PROFILES`), not structure — avoids double-maintaining service definitions across two files. Trade-off: host bind mounts (`./dags`, `./logs`, `./dbt_project`, etc.) are shared between both stacks, so avoid running `dbt_pipeline` in both at once. |

---

## Stopping / Resetting

```bash
# Stop containers (keeps data volumes)
docker compose down

# Full reset — removes ALL data including the warehouse
docker compose down -v

# Prod stack equivalents
docker compose -p airflow-dbt-prod --env-file .env.prod down
docker compose -p airflow-dbt-prod --env-file .env.prod down -v
```
