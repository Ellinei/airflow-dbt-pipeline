# Phase 1: Testing & CI/CD Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a pytest suite (DAG-import smoke tests + targeted business-logic unit tests), fill the empty dbt custom-test directories, and wire up a GitHub Actions CI workflow that gates pushes/PRs on both passing.

**Architecture:** dbt-direct CI decoupled from the Docker image — a GitHub Actions job with a bare `pgvector/pgvector:pg15` service container, `apache-airflow`/`dbt-core` installed via pip (same `requirements.txt` + Airflow constraints URL the Dockerfile uses), running `ruff check` then `pytest`. Two small refactors (`_ingest_olist_files`, `_extract_descriptions_from_manifest`) make previously-nested TaskFlow closures independently importable/testable. A hand-crafted ~20-30-row-per-table Olist sample fixture lets CI exercise the real ingest → dbt build → dbt test path without the 120MB Kaggle download.

**Tech Stack:** pytest 8.x, ruff, SQLAlchemy (already a dependency), GitHub Actions.

## Global Constraints

- Design spec: `docs/superpowers/specs/2026-07-17-phase1-testing-ci-design.md` (read it for full rationale — this plan implements it).
- Python 3.12, Airflow 2.9.1, dbt-core 1.8.0 — same versions as `requirements.txt`/`Dockerfile` (Phase 0).
- Airflow constraints URL (use exactly this string everywhere it's needed): `https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.12.txt`
- Test depth: smoke tests (DAG import) + targeted business-logic unit tests. **No** full Airflow-integration tests (no scheduler/webserver stack in CI) — this was an explicit user decision, don't add it back.
- Olist CI sample data lives at `tests/fixtures/olist_sample/` — **never** under `data/olist/` (gitignored for the real Kaggle data via `data/olist/*.csv`; a fixture placed there is silently swallowed).
- No coverage badges or third-party coverage services — terminal output only, not requested.
- Local dev Postgres: this project already has `postgres_warehouse` exposed at `localhost:5433` via `docker compose up -d postgres_warehouse`. Every task below that needs a live Postgres assumes this is running and these env vars are exported:
  ```bash
  export WAREHOUSE_DB_HOST=localhost
  export WAREHOUSE_DB_PORT=5433
  export WAREHOUSE_DB_USER=warehouse
  export WAREHOUSE_DB_PASSWORD=warehouse
  export WAREHOUSE_DB_NAME=warehouse
  ```
  (Read these from your own `.env` if the values differ — Phase 0 rotated some passwords but left `WAREHOUSE_DB_USER`/`WAREHOUSE_DB_PASSWORD`/`WAREHOUSE_DB_NAME` unchanged at `warehouse`/`warehouse`/`warehouse`.)
- Environment setup (do this once, before Task 1): from repo root,
  ```bash
  pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.12.txt" \
    "apache-airflow==2.9.1" -r requirements.txt
  pip install --no-cache-dir --no-deps "dbt-postgres==1.8.0" "pandas==2.1.4"
  ```
  This exact combination (fresh `apache-airflow` + this project's pinned extras, via the constraints file) was verified empirically during planning: clean install, ~5 min, no resolver backtracking, `SQLAlchemy` lands at `1.4.52` as required. `requirements-dev.txt` (Task 1) adds pytest/ruff on top of this.

---

### Task 1: Tooling foundation — pyproject.toml + requirements-dev.txt

**Files:**
- Create: `pyproject.toml`
- Create: `requirements-dev.txt`

**Interfaces:**
- Produces: a `pytest` command that discovers tests under `tests/` and can import `dags.*` modules; a `ruff check .` command. Every later task relies on both being installed and configured.

- [ ] **Step 1: Create `requirements-dev.txt`**

```
pytest>=8.0,<9.0
ruff>=0.5,<1.0
```

- [ ] **Step 2: Create `pyproject.toml`**

```toml
[tool.pytest.ini_options]
pythonpath = ["."]
testpaths = ["tests"]

[tool.ruff]
line-length = 100
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I"]
```

- [ ] **Step 3: Install and verify**

Run (from repo root, after the Global Constraints environment setup above):
```bash
pip install --no-cache-dir -r requirements-dev.txt
mkdir -p tests
echo "def test_placeholder(): assert True" > tests/test_placeholder.py
pytest -v
```
Expected: `1 passed`. This just proves `pyproject.toml`'s `pythonpath`/`testpaths` config works before any real test depends on it.

- [ ] **Step 4: Remove the placeholder and commit**

```bash
rm tests/test_placeholder.py
git add pyproject.toml requirements-dev.txt
git commit -m "Add pytest/ruff tooling config"
```

---

### Task 2: DAG portability + DAG-import smoke test

**Files:**
- Modify: `dags/dbt_pipeline_dag.py` (constants section, lines 37-40)
- Modify: `dbt_project/profiles.yml`
- Create: `tests/conftest.py`
- Create: `tests/test_dag_integrity.py`

**Interfaces:**
- Produces: `warehouse_engine` pytest fixture (session-scoped SQLAlchemy engine, ensures the `engineer` Postgres role exists) — Tasks 3, 4, 6 depend on this fixture.
- Produces: session-scoped autouse `dbt deps` fixture in `conftest.py` — every later dbt-touching test relies on `dbt_project/dbt_packages/` being populated.

**Why this task exists:** `dbt_pipeline_dag.py` hardcodes `DBT_PROJECT_PATH = Path("/opt/airflow/dbt_project")` and `DBT_EXECUTABLE = Path("/home/airflow/.local/bin/dbt")` — paths that only exist inside the Docker container. Importing this DAG file triggers Cosmos's `LoadMode.DBT_LS`, which shells out to `dbt ls` using these paths **at DAG-parse time** (not deferred to task execution) — so a plain `DagBag` import fails outside Docker unless these are portable. Verified empirically during planning: a fresh `apache-airflow==2.9.1` install with no prior Airflow config imports a `DagBag` cleanly (no `AIRFLOW_HOME` setup needed) — the only blocker is these two hardcoded paths. Similarly, `dbt_project/profiles.yml` hardcodes `host: postgres_warehouse` (the Docker Compose service name), unreachable outside that network — CI needs to reach the Postgres service container via `localhost`.

- [ ] **Step 1: Make `DBT_PROJECT_PATH`/`DBT_EXECUTABLE` portable in `dags/dbt_pipeline_dag.py`**

Current code (lines 26-40):
```python
import os
from datetime import datetime
from pathlib import Path

import requests

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode

# ── Paths (inside the Airflow containers) ─────────────────────────────────────
DBT_PROJECT_PATH = Path("/opt/airflow/dbt_project")
DBT_PROFILES_PATH = DBT_PROJECT_PATH                # profiles.yml lives here
DBT_EXECUTABLE = Path("/home/airflow/.local/bin/dbt")
```

Replace with:
```python
import os
import shutil
from datetime import datetime
from pathlib import Path

import requests

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode

# ── Paths ──────────────────────────────────────────────────────────────────────
# Computed relative to this file rather than hardcoded to the Docker container
# path, so DAG parsing also works in CI (dags/../dbt_project resolves to
# /opt/airflow/dbt_project inside the container — same mount topology — and to
# <repo_root>/dbt_project when running directly from a checkout).
DBT_PROJECT_PATH = Path(__file__).resolve().parent.parent / "dbt_project"
DBT_PROFILES_PATH = DBT_PROJECT_PATH                # profiles.yml lives here
# shutil.which finds dbt on PATH (true in CI, and also true inside the Docker
# container since pip install puts it on the airflow user's PATH); falls back
# to the container's known install location if not found on PATH.
DBT_EXECUTABLE = Path(shutil.which("dbt") or "/home/airflow/.local/bin/dbt")
```

- [ ] **Step 2: Parameterize `dbt_project/profiles.yml` host/port**

Current file:
```yaml
dbt_warehouse:
  target: dev
  outputs:
    dev:
      type: postgres
      host: postgres_warehouse          # Docker service name — resolves inside the network
      port: 5432                        # Internal container port (not the host 5433)
      user: "{{ env_var('WAREHOUSE_DB_USER') }}"
      password: "{{ env_var('WAREHOUSE_DB_PASSWORD') }}"
      dbname: "{{ env_var('WAREHOUSE_DB_NAME') }}"
      schema: public                    # Default schema; models override via +schema config
      threads: 4
      connect_timeout: 10
```

Replace with:
```yaml
dbt_warehouse:
  target: dev
  outputs:
    dev:
      type: postgres
      # Defaults preserve the existing in-container behavior (Docker service
      # name + internal port); override via WAREHOUSE_DB_HOST/PORT for CI or
      # host-side dbt CLI usage (matches the pattern already used by
      # rag/query.py since Phase 0).
      host: "{{ env_var('WAREHOUSE_DB_HOST', 'postgres_warehouse') }}"
      port: "{{ env_var('WAREHOUSE_DB_PORT', '5432') | as_number }}"
      user: "{{ env_var('WAREHOUSE_DB_USER') }}"
      password: "{{ env_var('WAREHOUSE_DB_PASSWORD') }}"
      dbname: "{{ env_var('WAREHOUSE_DB_NAME') }}"
      schema: public                    # Default schema; models override via +schema config
      threads: 4
      connect_timeout: 10
```

- [ ] **Step 3: Create `tests/conftest.py`**

```python
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest
import sqlalchemy

REPO_ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = REPO_ROOT / "dbt_project"


@pytest.fixture(scope="session", autouse=True)
def dbt_deps():
    """Install dbt packages (dbt_utils) once per test session. Cosmos's
    LoadMode.DBT_LS shells out to `dbt ls` at DAG-parse time, which fails if
    dbt_project/dbt_packages/ isn't populated yet — mirrors what airflow-init
    does in docker-compose before the scheduler ever parses DAGs."""
    dbt_exe = shutil.which("dbt")
    assert dbt_exe, "dbt not found on PATH — install project requirements first"
    subprocess.run(
        [dbt_exe, "deps", "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)],
        check=True,
    )


@pytest.fixture(scope="session")
def warehouse_engine():
    """SQLAlchemy engine for the test warehouse Postgres (CI service
    container, or the local postgres_warehouse — same WAREHOUSE_DB_* env vars
    the DAGs themselves use). Also ensures the `engineer` role exists: dbt's
    grant_select post-hook (dbt_project/macros/grant_select.sql) runs on
    every model build and fails if the role is missing, and a fresh database
    has no roles until governance/setup_roles.sql is run — which this test
    suite intentionally doesn't do (that's a production/manual concern, not
    a CI one)."""
    db_user = os.getenv("WAREHOUSE_DB_USER", "warehouse")
    db_password = os.getenv("WAREHOUSE_DB_PASSWORD", "warehouse")
    db_name = os.getenv("WAREHOUSE_DB_NAME", "warehouse")
    db_host = os.getenv("WAREHOUSE_DB_HOST", "localhost")
    db_port = os.getenv("WAREHOUSE_DB_PORT", "5432")
    engine = sqlalchemy.create_engine(
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text(
            "DO $$ BEGIN CREATE ROLE engineer NOLOGIN; "
            "EXCEPTION WHEN duplicate_object THEN NULL; END $$;"
        ))
    yield engine
    engine.dispose()
```

- [ ] **Step 4: Create `tests/test_dag_integrity.py`**

```python
from __future__ import annotations

from pathlib import Path

from airflow.models import DagBag

REPO_ROOT = Path(__file__).resolve().parent.parent
DAGS_DIR = REPO_ROOT / "dags"

EXPECTED_DAG_IDS = {"dbt_pipeline", "mlflow_training", "mlflow_training_olist", "rag_index"}


def test_no_dag_import_errors():
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert dagbag.import_errors == {}, dagbag.import_errors


def test_expected_dags_present():
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert EXPECTED_DAG_IDS.issubset(set(dagbag.dags.keys()))


def test_every_dag_has_at_least_one_task():
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    for dag_id, dag in dagbag.dags.items():
        assert len(dag.tasks) > 0, f"{dag_id} has no tasks"
```

- [ ] **Step 5: Run tests to verify they pass**

Ensure `postgres_warehouse` is running and the env vars from Global Constraints are exported, then:
```bash
docker compose up -d postgres_warehouse
pytest tests/test_dag_integrity.py -v
```
Expected: `3 passed`. If `test_no_dag_import_errors` fails, print `dagbag.import_errors` — the most likely cause is `dbt` not being resolvable (check `pip install ... dbt-core==1.8.0` ran) or the Postgres service not reachable (check `WAREHOUSE_DB_HOST`/`PORT` env vars).

- [ ] **Step 6: Commit**

```bash
git add dags/dbt_pipeline_dag.py dbt_project/profiles.yml tests/conftest.py tests/test_dag_integrity.py
git commit -m "Make DAG paths portable outside Docker, add DAG-import smoke tests"
```

---

### Task 3: Extract `_ingest_olist_files` + Olist CI sample fixture + unit tests

**Files:**
- Modify: `dags/dbt_pipeline_dag.py` (the `ingest_olist` task, lines ~42-168)
- Create: `tests/fixtures/olist_sample/olist_customers_dataset.csv`
- Create: `tests/fixtures/olist_sample/olist_orders_dataset.csv`
- Create: `tests/fixtures/olist_sample/olist_order_items_dataset.csv`
- Create: `tests/fixtures/olist_sample/olist_order_payments_dataset.csv`
- Create: `tests/fixtures/olist_sample/olist_order_reviews_dataset.csv`
- Create: `tests/fixtures/olist_sample/olist_products_dataset.csv`
- Create: `tests/fixtures/olist_sample/olist_sellers_dataset.csv`
- Create: `tests/fixtures/olist_sample/olist_geolocation_dataset.csv`
- Create: `tests/fixtures/olist_sample/product_category_name_translation.csv`
- Create: `tests/test_ingest_olist.py`

**Interfaces:**
- Consumes: `warehouse_engine` fixture (Task 2).
- Produces: `_ingest_olist_files(engine, data_dir: Path, files_map: dict[str, str]) -> dict[str, int]` in `dags/dbt_pipeline_dag.py` — Task 6 depends on this exact signature. Produces the fixture CSVs at `tests/fixtures/olist_sample/` — Task 6 depends on this exact directory referentially validating against the real dbt models/tests (customer IDs, order IDs, product IDs, seller IDs all cross-reference correctly; `order_status`/`payment_type`/`review_score` values are within the accepted-values lists in `dbt_project/models/olist_staging/schema.yml`).

- [ ] **Step 1: Extract `_ingest_olist_files` in `dags/dbt_pipeline_dag.py`**

Current code (the `ingest_olist` task, full body):
```python
    # ── Step 0: ingest the real-world Olist dataset into the raw schema ───────
    @task
    def ingest_olist() -> dict:
        """Load the 9 Olist CSVs into Postgres schema `raw` (literal — distinct
        from dbt's own public_raw seed schema). Idempotent: each table's data
        is truncated and reloaded in its own transaction on every run (never
        dropped — dbt's stg_olist_* views depend on these tables after the
        first run, and a plain DROP TABLE fails once dependent views exist)."""
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
                table_exists = conn.execute(
                    sqlalchemy.text(
                        "SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema = 'raw' AND table_name = :table_name"
                    ),
                    {"table_name": table_name},
                ).fetchone()
                if table_exists:
                    conn.execute(sqlalchemy.text(f'TRUNCATE TABLE raw."{table_name}"'))
                df.to_sql(
                    table_name,
                    con=conn,
                    schema="raw",
                    if_exists="append",
                    index=False,
                    chunksize=10_000,
                    method="multi",
                )
            row_counts[table_name] = len(df)

        return row_counts

    ingest = ingest_olist()
```

Replace with:
```python
    ingest = ingest_olist()
```

And add this, module-level, right after the `OLIST_FILES` dict definition (before `PROFILE_CONFIG = ProfileConfig(...)`):
```python
# Zip-code-prefix columns must stay strings or pandas drops leading zeros on
# Brazilian CEP codes (e.g. "01046" -> 1046).
ZIP_DTYPE_OVERRIDES = {
    "olist_customers_dataset.csv": {"customer_zip_code_prefix": str},
    "olist_sellers_dataset.csv": {"seller_zip_code_prefix": str},
    "olist_geolocation_dataset.csv": {"geolocation_zip_code_prefix": str},
}


def _ingest_olist_files(engine, data_dir: Path, files_map: dict[str, str]) -> dict[str, int]:
    """Load each Olist CSV in files_map into Postgres schema `raw`, truncating
    each table first if it already exists (idempotent — never drops, since
    dbt's stg_olist_* views depend on these tables after the first run, and a
    plain DROP TABLE fails once dependent views exist). Returns
    {table_name: row_count}. Pulled out of the ingest_olist task body so it's
    directly unit-testable with a fixture data_dir."""
    import pandas as pd
    import sqlalchemy

    missing = [f for f in files_map if not (data_dir / f).exists()]
    if missing:
        raise FileNotFoundError(
            f"Missing Olist CSV(s) in {data_dir}: {', '.join(missing)}. "
            "Download the Kaggle 'Brazilian E-Commerce Public Dataset by "
            "Olist' (olistbr/brazilian-ecommerce) and place all 9 files "
            "there — see data/olist/README.md."
        )

    with engine.begin() as conn:
        conn.execute(sqlalchemy.text("CREATE SCHEMA IF NOT EXISTS raw"))

    row_counts = {}
    for filename, table_name in files_map.items():
        df = pd.read_csv(
            data_dir / filename,
            dtype=ZIP_DTYPE_OVERRIDES.get(filename),
        )
        with engine.begin() as conn:
            table_exists = conn.execute(
                sqlalchemy.text(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = 'raw' AND table_name = :table_name"
                ),
                {"table_name": table_name},
            ).fetchone()
            if table_exists:
                conn.execute(sqlalchemy.text(f'TRUNCATE TABLE raw."{table_name}"'))
            df.to_sql(
                table_name,
                con=conn,
                schema="raw",
                if_exists="append",
                index=False,
                chunksize=10_000,
                method="multi",
            )
        row_counts[table_name] = len(df)

    return row_counts
```

And add this task definition where `ingest_olist()` used to be defined (inside the `dbt_pipeline()` DAG factory function, replacing the block removed above):
```python
    # ── Step 0: ingest the real-world Olist dataset into the raw schema ───────
    @task
    def ingest_olist() -> dict:
        """Thin Airflow wrapper — see _ingest_olist_files for the actual
        loading logic (module-level, independently unit-tested)."""
        import sqlalchemy

        db_user = os.getenv("WAREHOUSE_DB_USER", "warehouse")
        db_password = os.getenv("WAREHOUSE_DB_PASSWORD", "warehouse")
        db_name = os.getenv("WAREHOUSE_DB_NAME", "warehouse")
        engine = sqlalchemy.create_engine(
            f"postgresql+psycopg2://{db_user}:{db_password}@postgres_warehouse:5432/{db_name}"
        )
        return _ingest_olist_files(engine, OLIST_DATA_DIR, OLIST_FILES)

    ingest = ingest_olist()
```

- [ ] **Step 2: Create the 9 Olist CI sample fixture CSVs**

These are small (~2-5 rows), hand-crafted, referentially-consistent (customer/order/product/seller IDs cross-reference correctly), and use values within every `accepted_values` test's allowed list in `dbt_project/models/olist_staging/schema.yml` (`order_status`: created/invoiced/processing/unavailable/shipped/delivered/canceled/approved; `payment_type`: credit_card/boleto/voucher/debit_card/not_defined; `review_score`: 1-5).

`tests/fixtures/olist_sample/olist_customers_dataset.csv`:
```
customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state
cust_id_1,cust_unique_1,01001,sao paulo,SP
cust_id_2,cust_unique_1,01001,sao paulo,SP
cust_id_3,cust_unique_2,20040,rio de janeiro,RJ
cust_id_4,cust_unique_3,30140,belo horizonte,MG
cust_id_5,cust_unique_4,40010,salvador,BA
```

`tests/fixtures/olist_sample/olist_orders_dataset.csv`:
```
order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,order_delivered_carrier_date,order_delivered_customer_date,order_estimated_delivery_date
order_1,cust_id_1,delivered,2024-01-05 10:00:00,2024-01-05 11:00:00,2024-01-06 09:00:00,2024-01-10 14:00:00,2024-01-15 00:00:00
order_2,cust_id_3,delivered,2024-01-08 09:30:00,2024-01-08 10:00:00,2024-01-09 08:00:00,2024-01-12 16:00:00,2024-01-18 00:00:00
order_3,cust_id_2,delivered,2024-02-01 12:00:00,2024-02-01 12:30:00,2024-02-02 10:00:00,2024-02-06 13:00:00,2024-02-12 00:00:00
order_4,cust_id_4,shipped,2024-02-10 15:00:00,2024-02-10 15:30:00,2024-02-11 09:00:00,,2024-02-20 00:00:00
order_5,cust_id_5,canceled,2024-02-15 08:00:00,,,,2024-02-25 00:00:00
```

`tests/fixtures/olist_sample/olist_order_items_dataset.csv`:
```
order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value
order_1,1,prod_1,seller_1,2024-01-06 00:00:00,150.00,15.00
order_2,1,prod_2,seller_2,2024-01-09 00:00:00,80.00,10.00
order_3,1,prod_1,seller_1,2024-02-02 00:00:00,200.00,20.00
order_4,1,prod_3,seller_2,2024-02-11 00:00:00,50.00,8.00
order_5,1,prod_2,seller_1,2024-02-16 00:00:00,60.00,9.00
```

`tests/fixtures/olist_sample/olist_order_payments_dataset.csv`:
```
order_id,payment_sequential,payment_type,payment_installments,payment_value
order_1,1,credit_card,3,165.00
order_2,1,boleto,1,90.00
order_3,1,credit_card,2,220.00
order_4,1,voucher,1,58.00
order_5,1,credit_card,1,69.00
```

`tests/fixtures/olist_sample/olist_order_reviews_dataset.csv`:
```
review_id,order_id,review_score,review_comment_title,review_comment_message,review_creation_date,review_answer_timestamp
rev_1,order_1,5,great,fast delivery,2024-01-11 00:00:00,2024-01-12 00:00:00
rev_2,order_2,4,good,as expected,2024-01-13 00:00:00,2024-01-14 00:00:00
rev_3,order_3,5,excellent,will buy again,2024-02-07 00:00:00,2024-02-08 00:00:00
rev_4,order_4,3,ok,a bit slow,2024-02-19 00:00:00,2024-02-20 00:00:00
rev_5,order_5,1,bad,order was canceled,2024-02-26 00:00:00,2024-02-27 00:00:00
```

`tests/fixtures/olist_sample/olist_products_dataset.csv`:
```
product_id,product_category_name,product_name_lenght,product_description_lenght,product_photos_qty,product_weight_g,product_length_cm,product_height_cm,product_width_cm
prod_1,informatica_acessorios,45,200,2,500,20,10,15
prod_2,beleza_saude,38,150,1,300,15,8,12
prod_3,cama_mesa_banho,50,180,3,1200,40,20,30
```

`tests/fixtures/olist_sample/olist_sellers_dataset.csv`:
```
seller_id,seller_zip_code_prefix,seller_city,seller_state
seller_1,04001,sao paulo,SP
seller_2,01310,sao paulo,SP
```

`tests/fixtures/olist_sample/olist_geolocation_dataset.csv`:
```
geolocation_zip_code_prefix,geolocation_lat,geolocation_lng,geolocation_city,geolocation_state
01001,-23.550,-46.633,sao paulo,SP
01001,-23.551,-46.634,sao paulo,SP
20040,-22.906,-43.172,rio de janeiro,RJ
30140,-19.917,-43.934,belo horizonte,MG
40010,-12.971,-38.501,salvador,BA
04001,-23.561,-46.652,sao paulo,SP
01310,-23.561,-46.656,sao paulo,SP
```

`tests/fixtures/olist_sample/product_category_name_translation.csv`:
```
product_category_name,product_category_name_english
informatica_acessorios,computers_accessories
beleza_saude,health_beauty
cama_mesa_banho,bed_bath_table
```

- [ ] **Step 3: Create `tests/test_ingest_olist.py`**

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_ingest_olist.py -v
```
Expected: `3 passed`.

- [ ] **Step 5: Commit**

```bash
git add dags/dbt_pipeline_dag.py tests/fixtures/olist_sample tests/test_ingest_olist.py
git commit -m "Extract _ingest_olist_files, add Olist CI sample fixture and unit tests"
```

---

### Task 4: Extract `_extract_descriptions_from_manifest` + rag_index tests

**Files:**
- Modify: `dags/rag_index_dag.py` (the `extract_descriptions` task, lines ~90-120, and the `ensure_schema` task, lines ~40-88)
- Create: `tests/test_rag_index.py`

**Interfaces:**
- Consumes: `warehouse_engine` fixture (Task 2).
- Produces: `_extract_descriptions_from_manifest(manifest: dict) -> list[dict]` in `dags/rag_index_dag.py`.
- Produces: `_ensure_catalog_embeddings_schema(engine) -> None` in `dags/rag_index_dag.py` — a second, smaller extraction (see Step 1b) so the idempotency test reuses the exact production DDL instead of duplicating it.

**Why this task exists (correction from the design spec):** the spec assumed Airflow's TaskFlow decorator exposes the original callable so no refactor was needed for `extract_descriptions`. That's wrong — `extract_descriptions` is a local variable inside the `rag_index()` closure, never bound to any module-level or DAG-reachable name, so it's not actually importable as written. It also reads from the hardcoded module-level `MANIFEST_PATH` constant rather than taking a parameter, so even if it were reachable, you couldn't point it at a test fixture. Same extraction pattern as Task 3 fixes both problems.

**Pre-flight correction:** the first draft of this task had the idempotency test re-declare `catalog_embeddings`'s DDL inline (duplicating `ensure_schema`'s DDL verbatim, a defect pattern — see the plan's review rubric). Fixed by also extracting `ensure_schema`'s DDL into `_ensure_catalog_embeddings_schema(engine)` (Step 1b below), which both the task and the test call — no duplication, and the test now exercises the literal production DDL rather than a hand-copied approximation of it.

- [ ] **Step 1: Extract `_extract_descriptions_from_manifest` in `dags/rag_index_dag.py`**

Current code (the `extract_descriptions` task, full body):
```python
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
```

Replace with (add the module-level function before the `@dag` decorator, right after the `WAREHOUSE_DSN` assignment; keep a thin `@task` wrapper in its original position inside `rag_index()`):

Module level (after `WAREHOUSE_DSN = ...`, before `@dag`):
```python
def _extract_descriptions_from_manifest(manifest: dict) -> list[dict]:
    """Pull model + column descriptions from a parsed dbt manifest dict.
    Pulled out of the extract_descriptions task body so it's directly
    unit-testable with a fixture manifest — no file I/O, no MANIFEST_PATH
    dependency."""
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
```

Inside `rag_index()` (replacing the old task body):
```python
    @task
    def extract_descriptions() -> list[dict]:
        """Thin Airflow wrapper — see _extract_descriptions_from_manifest for
        the actual parsing logic (module-level, independently unit-tested)."""
        if not MANIFEST_PATH.exists():
            raise FileNotFoundError(
                f"{MANIFEST_PATH} not found — run the dbt_pipeline DAG first."
            )
        manifest = json.loads(MANIFEST_PATH.read_text())
        return _extract_descriptions_from_manifest(manifest)
```

- [ ] **Step 1b: Extract `_ensure_catalog_embeddings_schema` in `dags/rag_index_dag.py`**

Current code (the `ensure_schema` task, full body):
```python
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

        -- Dedupe any rows a prior (pre-fix) run already duplicated, before the
        -- unique index below can be created. Ties on updated_at (all rows from
        -- one run share the same transaction timestamp) are broken by ctid.
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

        -- COALESCE(column_name, '') because model-level rows have column_name
        -- NULL, and a plain unique index treats every NULL as distinct — it
        -- would never dedupe those rows without the COALESCE.
        CREATE UNIQUE INDEX IF NOT EXISTS catalog_embeddings_unique_idx
            ON catalog_embeddings (source, model_name, COALESCE(column_name, ''));
        """
        engine = sqlalchemy.create_engine(WAREHOUSE_DSN)
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text(ddl))
            conn.commit()
```

Replace with (module level, placed right after `_extract_descriptions_from_manifest` from Step 1):
```python
def _ensure_catalog_embeddings_schema(engine) -> None:
    """Create extension + table + indexes if not already present (idempotent).
    Pulled out of the ensure_schema task body so tests can call the exact
    production DDL instead of duplicating it."""
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

    -- Dedupe any rows a prior (pre-fix) run already duplicated, before the
    -- unique index below can be created. Ties on updated_at (all rows from
    -- one run share the same transaction timestamp) are broken by ctid.
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

    -- COALESCE(column_name, '') because model-level rows have column_name
    -- NULL, and a plain unique index treats every NULL as distinct — it
    -- would never dedupe those rows without the COALESCE.
    CREATE UNIQUE INDEX IF NOT EXISTS catalog_embeddings_unique_idx
        ON catalog_embeddings (source, model_name, COALESCE(column_name, ''));
    """
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text(ddl))
        conn.commit()
```

And inside `rag_index()` (replacing the old task body):
```python
    @task
    def ensure_schema() -> None:
        """Thin Airflow wrapper — see _ensure_catalog_embeddings_schema."""
        import sqlalchemy

        engine = sqlalchemy.create_engine(WAREHOUSE_DSN)
        _ensure_catalog_embeddings_schema(engine)
```

- [ ] **Step 2: Create `tests/test_rag_index.py`**

```python
from __future__ import annotations

import sqlalchemy

from dags.rag_index_dag import _ensure_catalog_embeddings_schema, _extract_descriptions_from_manifest


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
```

- [ ] **Step 3: Run tests to verify they pass**

```bash
pytest tests/test_rag_index.py -v
```
Expected: `3 passed`.

- [ ] **Step 4: Commit**

```bash
git add dags/rag_index_dag.py tests/test_rag_index.py
git commit -m "Extract _extract_descriptions_from_manifest, add rag_index unit tests"
```

---

### Task 5: dbt custom tests — masked-view column tests + business-invariant singular tests

**Files:**
- Modify: `dbt_project/models/marts/schema.yml`
- Create: `dbt_project/tests/assert_customer_orders_subtotals_valid.sql`
- Create: `dbt_project/tests/assert_olist_customer_orders_subtotals_valid.sql`

**Interfaces:** none (pure dbt project changes, verified by Task 6's `dbt test` run — this task only needs `dbt run`/`dbt test` against whatever local data already exists to sanity-check syntax).

- [ ] **Step 1: Add column tests to `mart_customer_orders_masked` in `dbt_project/models/marts/schema.yml`**

Current (the `mart_customer_orders_masked` block):
```yaml
  - name: mart_customer_orders_masked
    description: >
      Analyst-facing view of mart_customer_orders with the email column masked
      (shows only the first character plus the domain, e.g. a****@example.com).
      The analyst role is granted SELECT on this view only — never on the raw mart.
    meta:
      owner: Ellinei
      team: Data Engineering
      tier: Tier 1
    tags:
      - mart
      - customers
      - PII-masked
    columns:
      - name: customer_id
        description: "Primary key — unique per customer."
      - name: email
        description: "Masked email — first character + **** + @domain."
      - name: lifetime_value
        description: "Sum of all order amounts across the customer's history."
      - name: total_orders
        description: "Total number of orders placed by the customer."
      - name: completed_orders
        description: "Count of orders with status = 'completed'."
      - name: cancelled_orders
        description: "Count of orders with status = 'cancelled'."
```

Replace with (adds `not_null`/`unique` on `customer_id`, matching the unmasked mart):
```yaml
  - name: mart_customer_orders_masked
    description: >
      Analyst-facing view of mart_customer_orders with the email column masked
      (shows only the first character plus the domain, e.g. a****@example.com).
      The analyst role is granted SELECT on this view only — never on the raw mart.
    meta:
      owner: Ellinei
      team: Data Engineering
      tier: Tier 1
    tags:
      - mart
      - customers
      - PII-masked
    columns:
      - name: customer_id
        description: "Primary key — unique per customer."
        tests:
          - not_null
          - unique
      - name: email
        description: "Masked email — first character + **** + @domain."
      - name: lifetime_value
        description: "Sum of all order amounts across the customer's history."
      - name: total_orders
        description: "Total number of orders placed by the customer."
      - name: completed_orders
        description: "Count of orders with status = 'completed'."
      - name: cancelled_orders
        description: "Count of orders with status = 'cancelled'."
```

- [ ] **Step 2: Create `dbt_project/tests/assert_customer_orders_subtotals_valid.sql`**

```sql
-- Business invariant: completed + cancelled orders can never exceed total
-- orders. Generic schema tests can't express a cross-column check like this
-- one, hence a singular test. Returns offending rows — the test fails if
-- this query returns any.
select
    customer_id,
    total_orders,
    completed_orders,
    cancelled_orders
from {{ ref('mart_customer_orders') }}
where completed_orders + cancelled_orders > total_orders
```

- [ ] **Step 3: Create `dbt_project/tests/assert_olist_customer_orders_subtotals_valid.sql`**

```sql
-- Business invariant: delivered + cancelled orders can never exceed total
-- orders. Returns offending rows — the test fails if this query returns any.
select
    customer_unique_id,
    total_orders,
    delivered_orders,
    cancelled_orders
from {{ ref('mart_olist_customer_orders') }}
where delivered_orders + cancelled_orders > total_orders
```

- [ ] **Step 4: Run dbt to verify the new tests pass against current data**

Requires `postgres_warehouse` running, `WAREHOUSE_DB_*` env vars exported (Global Constraints), and `dbt_pipeline`'s toy-demo seed + at least one prior full run so marts exist (or run seed+run now):
```bash
cd dbt_project
dbt deps --profiles-dir .
dbt seed --profiles-dir .
dbt run --profiles-dir . --select mart_customer_orders mart_customer_orders_masked
dbt test --profiles-dir . --select mart_customer_orders_masked assert_customer_orders_subtotals_valid
cd ..
```
Expected: all tests `PASS` (verified during planning against this exact seed data — see the design/plan derivation, no customer's `completed_orders + cancelled_orders` exceeds their `total_orders`). The Olist singular test (`assert_olist_customer_orders_subtotals_valid`) needs Olist data loaded first — it's exercised for real in Task 6; skip running it standalone here unless you already have Olist data loaded locally.

- [ ] **Step 5: Commit**

```bash
git add dbt_project/models/marts/schema.yml dbt_project/tests/assert_customer_orders_subtotals_valid.sql dbt_project/tests/assert_olist_customer_orders_subtotals_valid.sql
git commit -m "Add masked-view column tests and business-invariant singular tests"
```

---

### Task 6: Full dbt build test against the Olist CI sample

**Files:**
- Create: `tests/test_dbt_build.py`

**Interfaces:**
- Consumes: `warehouse_engine` fixture (Task 2), `_ingest_olist_files` + `OLIST_FILES` (Task 3), `tests/fixtures/olist_sample/` (Task 3), the two new singular tests + masked-view column tests (Task 5).

- [ ] **Step 1: Create `tests/test_dbt_build.py`**

```python
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from dags.dbt_pipeline_dag import OLIST_FILES, _ingest_olist_files

REPO_ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = REPO_ROOT / "dbt_project"
OLIST_SAMPLE_DIR = Path(__file__).resolve().parent / "fixtures" / "olist_sample"


def _run_dbt(*args: str) -> None:
    dbt_exe = shutil.which("dbt")
    assert dbt_exe, "dbt not found on PATH — install project requirements first"
    subprocess.run(
        [dbt_exe, *args, "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)],
        check=True,
    )


def test_dbt_seed_run_test_succeed_against_toy_demo_and_olist_sample(warehouse_engine):
    _ingest_olist_files(warehouse_engine, OLIST_SAMPLE_DIR, OLIST_FILES)
    _run_dbt("seed")
    _run_dbt("run")
    _run_dbt("test")
```

- [ ] **Step 2: Run the test to verify it passes**

```bash
pytest tests/test_dbt_build.py -v
```
Expected: `1 passed`. This runs the real `dbt seed && dbt run && dbt test` (all 69+ existing tests plus the 2 new singular tests from Task 5) against both the toy-demo seeds and the Olist CI sample fixture — if any dbt test fails (including the two new business-invariant tests against the fixture data), `subprocess.run(..., check=True)` raises `CalledProcessError` and this test fails with the dbt CLI's own output showing which test failed.

- [ ] **Step 3: Run the full test suite together to confirm no cross-test interference**

```bash
pytest -v
```
Expected: all tests across `tests/test_dag_integrity.py`, `tests/test_ingest_olist.py`, `tests/test_rag_index.py`, `tests/test_dbt_build.py` pass together (order-independent — `_ingest_olist_files`'s truncate-and-reload means rerunning it between tests is harmless).

- [ ] **Step 4: Commit**

```bash
git add tests/test_dbt_build.py
git commit -m "Add full dbt build/test gate against toy demo + Olist CI sample"
```

---

### Task 7: GitHub Actions CI workflow

**Files:**
- Create: `.github/workflows/ci.yml`

**Interfaces:** none — this is the final task, wiring together everything from Tasks 1-6 into automated CI.

- [ ] **Step 1: Create `.github/workflows/ci.yml`**

```yaml
name: CI

on:
  push:
    branches: [master]
  pull_request:
    branches: [master]

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      postgres:
        image: pgvector/pgvector:pg15
        env:
          POSTGRES_USER: warehouse
          POSTGRES_PASSWORD: warehouse
          POSTGRES_DB: warehouse
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

    env:
      WAREHOUSE_DB_USER: warehouse
      WAREHOUSE_DB_PASSWORD: warehouse
      WAREHOUSE_DB_NAME: warehouse
      WAREHOUSE_DB_HOST: localhost
      WAREHOUSE_DB_PORT: "5432"

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: "pip"

      - name: Install dependencies
        run: |
          pip install --no-cache-dir \
            --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.12.txt" \
            "apache-airflow==2.9.1" -r requirements.txt
          pip install --no-cache-dir --no-deps "dbt-postgres==1.8.0" "pandas==2.1.4"
          pip install --no-cache-dir -r requirements-dev.txt

      - name: Lint
        run: ruff check .

      - name: Run tests
        run: pytest -v
```

- [ ] **Step 2: Push the branch and verify the workflow runs green on GitHub**

```bash
git add .github/workflows/ci.yml
git commit -m "Add GitHub Actions CI workflow"
git push origin master
```
Then check the Actions tab on `github.com/Ellinei/airflow-dbt-pipeline` (or `gh run watch` if the `gh` CLI is available) — expected: the `test` job completes green. If it fails, read the failed step's log; the most likely first-time failure is a package version drift between local dev and CI (fix by re-verifying the exact install command from Global Constraints locally first).

---

## Self-Review Notes

**Spec coverage:** All 5 spec sections have a task — refactor (Tasks 3, 4), pytest suite (Tasks 2, 3, 4, 6), dbt custom tests (Task 5), Olist CI sample data (Task 3), CI workflow (Task 7). Tooling (Task 1) wasn't a separate spec section but is a prerequisite the spec implied ("ruff", "pyproject.toml").

**Corrections made from the spec during planning** (documented inline in the relevant tasks, not hidden): `rag_index_dag.py`'s `extract_descriptions` does need a refactor after all (Task 4) — the spec's assumption that TaskFlow exposes it directly was wrong. Two additional portability fixes not in the original spec were discovered by reading the actual DAG code: `DBT_PROJECT_PATH`/`DBT_EXECUTABLE` hardcoded container paths (Task 2), and `profiles.yml`'s hardcoded `host: postgres_warehouse` (Task 2) — both would have made the DAG-import test and dbt-build test fail in CI immediately. Both were verified empirically (fresh-install spike, DagBag-import spike) before being written into this plan.

**Pre-flight review correction (before Task 1 dispatch):** Task 4's original draft had the upsert-idempotency test re-declare `catalog_embeddings`'s DDL inline, duplicating `ensure_schema`'s DDL verbatim — a defect pattern the review rubric explicitly flags. Fixed by extracting a second helper, `_ensure_catalog_embeddings_schema(engine)`, reused by both the task and the test (Task 4, Step 1b).

**Type/interface consistency:** `_ingest_olist_files(engine, data_dir: Path, files_map: dict[str, str]) -> dict[str, int]` — same signature used in Tasks 3 and 6. `_extract_descriptions_from_manifest(manifest: dict) -> list[dict]` — same signature used in Task 4 (defined and tested in the same task). `warehouse_engine` fixture — same name/behavior referenced by Tasks 3, 4, 6, defined once in Task 2.
