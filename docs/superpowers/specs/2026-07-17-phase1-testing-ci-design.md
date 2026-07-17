# Phase 1: Testing & CI/CD — Design

## Context

Phase 0 (secrets rotation, `rag_index` idempotency fix, dependency lockfile — see
`docs/superpowers/specs/` history and commit `80bc418`) established the foundation for the
project's "production grade" roadmap. This is Phase 1: the project currently has zero automated
tests outside dbt's own schema tests, and zero CI/CD (confirmed by the original 3-agent audit and
re-confirmed while designing this phase). Every push to `master` — a public repo
(`github.com/Ellinei/airflow-dbt-pipeline`) — is unverified until someone manually runs the
stack. This phase adds a pytest suite, fills the empty dbt custom-test directories, and wires up
GitHub Actions so pushes/PRs are gated on both passing.

**Scope decisions made with the user:**
- Test depth: **smoke tests (DAG import/parse) + targeted business-logic unit tests**, not full
  Airflow-integration tests (no spinning up the full scheduler/webserver stack in CI).
- Olist CI data: **commit a small synthetic sample** (~20-30 referentially-consistent rows per
  table) so CI can exercise the real `ingest_olist` → dbt build → dbt test path, not just the
  toy demo.
- CI architecture: **dbt-direct CI, decoupled from the full Docker image** — a bare Postgres
  service container in GitHub Actions, dbt-core/Airflow installed via pip (same
  `requirements.txt` + constraints URL as the Dockerfile), no `docker compose build` in the hot
  path. A full-stack Docker build/boot check was explicitly deferred (not part of this phase) —
  the user chose to keep Phase 1 fast (~2-3 min) rather than add a ~10-15 min full-integration
  job.

**Explicitly out of scope for this phase:** DAG retries/alerting/idempotency-guard work (Phase
2), dev/prod environment separation (Phase 3), any full `docker compose` build/boot check in CI
(deferred, not scheduled to any phase yet — revisit if Dockerfile/compose drift becomes a real
problem), code coverage badges or third-party coverage services (not requested — terminal
coverage output only, if any).

---

## 1. Minimal refactor for testability

Every `@task`-decorated function in this project's 4 DAGs is a closure nested inside its
`@dag`-decorated factory function (standard Airflow TaskFlow style) — none of them are
independently importable. Read directly (not assumed) during design:

- **`dags/dbt_pipeline_dag.py`'s `ingest_olist`** has real, worth-testing logic: a
  missing-file check, per-table truncate-vs-fresh-load branching, and — the most bug-prone part
  — a zip-code dtype override dict that keeps Brazilian CEP codes as strings (a plain
  `pd.read_csv` would silently drop leading zeros). This gets extracted into a module-level
  function:
  ```python
  def _ingest_olist_files(engine, data_dir: Path, files_map: dict[str, str]) -> dict[str, int]:
      ...  # missing-file check, schema creation, per-table truncate+load, returns row counts
  ```
  `ingest_olist()` (the `@task`) becomes a thin wrapper that builds the engine and calls this.
  Same DSN/env-var behavior, same docstring — no behavior change, purely an extraction.

- **`dags/rag_index_dag.py`'s `extract_descriptions`** is already a pure function (manifest.json
  in, list of dicts out) — no extraction needed. Airflow's TaskFlow decorator exposes the
  original callable via `.function`, so it's testable as
  `rag_index_dag.extract_descriptions.function(...)` without any refactor.

- **`dags/mlflow_training_dag.py` / `dags/mlflow_training_olist_dag.py`** are almost entirely
  DB-read → sklearn-train → MLflow-log glue with no separable pure logic (the one candidate, the
  `test_size=max(1, int(len(df)*0.2))` line, is a single expression not worth extracting an
  abstraction for). These get smoke-test coverage only via the DAG-import test — no unit test
  file, and no refactor. Decided explicitly rather than left as a silent gap.

---

## 2. pytest suite

New `tests/` directory at the repo root (distinct from `dbt_project/tests/`, which is dbt's own
singular-test directory and is a different mechanism entirely).

| File | Covers | Needs live Postgres? |
|---|---|---|
| `tests/test_dag_integrity.py` | `DagBag` import for all 4 DAGs: no import errors, no duplicate `dag_id`s, each DAG has at least one task. Also exercises Cosmos's `dbt ls` subprocess call (`LoadMode.DBT_LS` runs at parse time), so a broken dbt project fails this test too. | Yes — Cosmos's `dbt ls` needs a real warehouse connection to resolve the profile, even though it doesn't run models. |
| `tests/test_ingest_olist.py` | `_ingest_olist_files` against the Olist CI sample fixture: zip-code columns stay strings after load, re-running the function on the same tables doesn't duplicate rows (truncate-and-reload), missing-file check raises `FileNotFoundError`. | Yes |
| `tests/test_rag_index.py` | `extract_descriptions` against a small fixture `manifest.json` (no DB) — model-level vs column-level entries, empty descriptions skipped. Plus a rerun-idempotency test for the upsert SQL (formalizing the manual check done in Phase 0): insert, update via conflict, assert row count stable and description updated. | Second test class needs Postgres; first does not. |
| `tests/test_dbt_build.py` | Loads the Olist CI sample via `_ingest_olist_files`, then shells out to `dbt seed && dbt run && dbt test` against both the toy demo and Olist models, asserting each subprocess exits 0. This is the "does the real dbt project actually build and pass its 69+ tests" gate. | Yes |

`conftest.py` provides a session-scoped fixture building a SQLAlchemy engine from
`WAREHOUSE_DB_*` env vars (same pattern the DAGs already use) — CI supplies these via the
Postgres service container's env, local runs read from `.env` (already true for everything else
in this project).

Mocking: `test_rag_index.py`'s `extract_descriptions` test needs no mocking (pure function, no
OpenAI call — that only happens in `embed_and_store`, which stays untested at the unit level
since it's a thin OpenAI-call + upsert wrapper; the upsert SQL itself is what's tested, directly
against Postgres, not through the OpenAI-calling function).

---

## 3. dbt custom tests

Fill the currently-empty `dbt_project/tests/` directories (flagged in the Phase 0 audit as
documentation/reality drift — the README already describes this directory as containing "custom
singular tests" that don't exist):

- **Column tests on `mart_customer_orders_masked`** (currently zero tests despite being the
  analyst-facing PII-masked view) — `not_null`/`unique` on `customer_id`, matching the
  equivalent tests already on the unmasked `mart_customer_orders`.
- **Singular tests** for business invariants that span multiple columns (generic schema tests
  can't express these; `dbt_utils.expression_is_true` only tests one column at a time):
  - `mart_customer_orders`: `completed_orders + cancelled_orders <= total_orders`
  - `mart_olist_customer_orders`: `delivered_orders + cancelled_orders <= total_orders`
    (confirmed `delivered_orders`/`cancelled_orders`/`total_orders` all exist on this model by
    reading `mart_olist_customer_orders.sql` during design)

---

## 4. Olist CI sample data

New `tests/fixtures/olist_sample/` — 9 CSVs matching the real Kaggle dataset's exact column
names/dtypes, ~20-30 rows each, hand-crafted with consistent cross-references (order IDs,
customer IDs, product IDs, seller IDs that actually join across tables) so the real dbt
relationship tests pass against it.

**Not placed under `data/olist/`** — that path is `data/olist/*.csv`-gitignored for the real
Kaggle download; a fixture placed there would be silently excluded from git.

---

## 5. CI workflow

New `.github/workflows/ci.yml`, one job, triggered on push and pull_request to `master`:

1. Postgres service container (`pgvector/pgvector:pg15`, matching the prod warehouse image).
2. Checkout, setup Python 3.12.
3. `pip install -r requirements.txt --constraint <airflow-constraints-url>` (same one the
   Dockerfile uses) `-r requirements-dev.txt` (new file: `pytest`, `ruff`).
4. `ruff check .`
5. `pytest`

New `pyproject.toml` (doesn't exist yet) holds `[tool.pytest.ini_options]` (testpaths, etc.) and
`[tool.ruff]` config in one file rather than two.

---

## Explicitly out of scope

Full `docker compose build`/boot check in CI (deferred, see Context). DAG retries/alerting
(Phase 2). Dev/prod dbt target separation (Phase 3). Coverage badges/third-party services.
Testing `mlflow_training_dag.py`/`_olist_dag.py` business logic beyond the smoke test (decided:
no separable pure logic worth extracting).
