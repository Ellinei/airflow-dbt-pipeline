# Airflow + dbt + PostgreSQL — Local Data Pipeline

A portfolio-grade data engineering project that wires together **Apache Airflow 2.9**, **dbt-core 1.8**, and **PostgreSQL 15** entirely inside Docker.

---

## Architecture

```
┌───────────────────────────────────────────────────────────────┐
│  Docker network                                               │
│                                                               │
│  ┌─────────────────┐     metadata      ┌──────────────────┐   │
│  │ Airflow         │ ◄──────────────► │ postgres_airflow │    │
│  │  · webserver    │    (port 5432)   │ (Airflow DB)     │    │
│  │  · scheduler    │                  └──────────────────┘    │
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

| Service               | Purpose                              | Host port |
|-----------------------|--------------------------------------|-----------|
| `airflow-webserver`   | Airflow UI                           | 8080      |
| `airflow-scheduler`   | DAG scheduling                       | —         |
| `airflow-init`        | One-shot DB migration + admin user   | —         |
| `postgres_airflow`    | Airflow metadata store               | 5432      |
| `postgres_warehouse`  | dbt target / data warehouse          | 5433      |

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

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (with Compose v2)
- ~4 GB RAM allocated to Docker

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
#    http://localhost:8080   login: admin / admin

# 5. Trigger the DAG manually from the UI, or wait for the daily schedule
```

### Run dbt manually inside the scheduler container

```bash
docker exec -it airflow_scheduler bash

# Inside the container:
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
Database: warehouse
User:     warehouse
Password: warehouse
```

---

## Project Structure

```
.
├── .env                          # Credentials (never commit to git)
├── docker-compose.yml            # All services
├── dags/
│   └── dbt_pipeline_dag.py       # Airflow DAG (Cosmos DbtTaskGroup)
├── dbt_project/
│   ├── dbt_project.yml           # dbt project config
│   ├── profiles.yml              # Warehouse connection (reads env vars)
│   ├── seeds/
│   │   ├── raw_customers.csv
│   │   └── raw_orders.csv
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_orders.sql
│   │   │   └── schema.yml        # not_null, unique, relationships tests
│   │   └── marts/
│   │       ├── mart_customer_orders.sql
│   │       └── schema.yml
│   └── tests/                    # Custom singular tests (add here)
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

---

## Stopping / Resetting

```bash
# Stop containers (keeps data volumes)
docker compose down

# Full reset — removes ALL data including the warehouse
docker compose down -v
```
