# Phase 3: Dev/Prod Environment Separation — Design

## Context

Phase 1 (pytest + CI — see `docs/superpowers/specs/2026-07-17-phase1-testing-ci-design.md`) and
Phase 2 (retries, alerting, idempotency — see
`docs/superpowers/specs/2026-07-18-phase2-retries-alerting-idempotency-design.md`) are merged and
pushed. Phase 2's design spec explicitly scoped dev/prod environment separation out as "Phase 3,
next on the project's production-grade roadmap." This is that phase.

**Scope decisions settled with the user via Q&A:**
- A second "prod" Docker Compose stack must run **simultaneously** with the existing "dev" stack on
  the same machine (own ports, containers, volumes) — not documented-but-not-running, not a real
  cloud target yet (cloud-deployable is a *later* goal; this phase just removes local-only
  hardcoding, it doesn't build cloud infra).
- One shared `docker-compose.yml` (no second/override compose file) — every dev/prod difference is
  a scalar env-var value, not structure.
- **Dev's invocation and identity must not change at all** — no `-p`/`COMPOSE_PROJECT_NAME` for
  dev, so it keeps Compose's current implicit project name and existing volumes/containers.
- Prod's service scope = dev's always-on core (`postgres_airflow`, `postgres_warehouse`,
  `airflow-init`, `airflow-webserver`, `airflow-scheduler`) **plus `mlflow-server`** — confirmed by
  reading the DAGs that it's a real runtime dependency of `mlflow_training_dag.py` and
  `mlflow_training_olist_dag.py` (`MLFLOW_TRACKING_URI`), unlike OpenMetadata which nothing depends
  on at runtime and stays dev-only/opt-in in both stacks.
- Prod uses independently-generated secrets via a new `.env.prod` (gitignored, user-created from a
  template — generating real secrets is not part of this implementation).

**Codebase research** (cross-checked against the running Docker Compose installation) found the
exact current state:
- **Exactly one piece of environment-conditional code exists anywhere**: `dags/dbt_pipeline_dag.py`
  (line 130) hardcodes `target_name="dev"` in a Cosmos `ProfileConfig`. None of the other 3 DAG
  files touch dbt/Cosmos or the string `"dev"` at all.
- `dbt_project/profiles.yml` has only a `dev` output block.
- `docker-compose.yml` hardcodes host ports (`5432`, `5433`, `8080`, `5000`) and static
  `container_name:` on `postgres_airflow`, `postgres_warehouse`, `airflow-init`,
  `airflow-webserver`, `airflow-scheduler`, `mlflow-server` — container names are Docker-daemon-
  global, not Compose-project-scoped, so two projects can't share one.
- Default Compose project name today (verified via `docker compose config`, no `-p` flag) is
  **`airflowdbtpipeline`** — matches the existing `airflowdbtpipeline_airflow_postgres_data` etc.
  volume names exactly, confirming dev must never get an explicit `-p`/`COMPOSE_PROJECT_NAME` or its
  current volumes become orphaned.
- Compose profile OR-semantics verified directly (read-only, no containers started): with today's
  `mlflow-server: profiles: ["mlops"]`, running `COMPOSE_PROFILES=mlops docker compose config
  --services` includes `mlflow-server`; unset, it doesn't. This validates the mechanism §2 relies on
  (adding a second profile name, `prod-core`, activated via `.env.prod`).
- `README.md:190` — `docker exec -it airflow_scheduler bash` depends on the literal `container_name`
  this phase removes; it would silently break. Fixed to `docker compose exec airflow-scheduler
  bash` (works regardless of project/`container_name`, and composes with `-p`/`--env-file` for
  targeting prod).
- `README.md:197` already uses `--port 8081` for an unrelated manual `dbt docs serve` example, so
  prod's webserver port uses `8090` instead of `8081` to avoid reader confusion (not a functional
  collision — that port is never published to the host — just documentation clarity).

## Approach

**One shared `docker-compose.yml`, env-var-parameterized, isolated via Compose project-name
prefixing.** Nothing about container-to-container traffic changes — internal ports, service-name DNS
(`postgres_warehouse`, `mlflow-server`, etc.), and connection strings like
`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` and `MLFLOW_TRACKING_URI` stay exactly as-is, because each
Compose project gets its own isolated network — "dev's `mlflow-server`" and "prod's `mlflow-server`"
are never ambiguous even sharing an internal hostname. Only two kinds of things change: **host-side
port mappings** (parameterized via `${VAR:-default}`, default = today's literal) and **static
`container_name:` keys** (removed, letting Compose auto-name containers per project).

**Rejected alternative:** a second `docker-compose.prod.yml` override file — explicitly ruled out by
the user; also would double-maintain every service definition for what are purely scalar
differences.

**Known limitation, accepted as out of scope:** host bind mounts (`./dags`, `./logs`,
`./dbt_project`, `./data`) are shared between the two stacks — only containers/volumes/ports are
isolated. Concurrently triggering `dbt_pipeline` in both stacks at once would race on the shared
`dbt_project/target/`/`dbt_packages/` directories. Documented as an operational caveat in the
README rather than engineered around (e.g. per-stack bind mount paths) — out of scope per the user's
framing of this phase as "avoid local-only hardcoding," not "fully isolate every shared resource."

---

## 1. dbt target wiring

`dbt_project/profiles.yml`: top-level `target: dev` → `target: "{{ env_var('DBT_TARGET', 'dev') }}"`,
plus a new `prod:` output block structurally identical to `dev:` (same `env_var()` names —
differentiation comes from which stack's env vars are live at runtime, not from different variable
names).

`dags/dbt_pipeline_dag.py`: `ProfileConfig(..., target_name="dev", ...)` →
`target_name=os.getenv("DBT_TARGET", "dev")` (`os` already imported and used elsewhere in the file).
This one variable covers both Cosmos's `DbtTaskGroup` (passes `--target` from
`PROFILE_CONFIG.target_name`) and the file's plain `BashOperator` dbt tasks (`dbt_deps`, `dbt_seed`,
`dbt_docs_generate`), which pass no `--target` and rely on `profiles.yml`'s top-level default — now
also `DBT_TARGET`-driven, so every dbt invocation in a stack is consistent.

## 2. `docker-compose.yml` parameterization

- `x-airflow-common-env`: add `DBT_TARGET: "${DBT_TARGET:-dev}"` next to the existing
  `WAREHOUSE_DB_USER`/`PASSWORD`/`NAME` forwarding.
- `postgres_airflow`: port → `"${AIRFLOW_DB_PORT:-5432}:5432"`; remove `container_name`.
- `postgres_warehouse`: port → `"${WAREHOUSE_DB_PORT:-5433}:5432"`; remove `container_name`. **Do
  not** add `WAREHOUSE_DB_PORT` to `x-airflow-common-env` — today it's forwarded to nothing (only
  USER/PASSWORD/NAME are), and `profiles.yml` already falls back to the container-internal port
  `5432` when unset in-container. Forwarding it would make in-container dbt dial the *host* mapping
  value instead of the real internal port, breaking every dbt connection in prod.
- `airflow-init`: remove `container_name` only; its `command:` block is untouched.
- `airflow-webserver`: port → `"${AIRFLOW_WEBSERVER_PORT:-8080}:8080"`; remove `container_name`.
- `airflow-scheduler`: remove `container_name` (no port mapping on this service today).
- `mlflow-server`: `profiles: ["mlops"]` → `profiles: ["mlops", "prod-core"]` (OR semantics,
  verified above); port → `"${MLFLOW_PORT:-5000}:5000"` (host side only — `--port 5000` in
  `command:` and the container side of the mapping stay untouched); remove `container_name` (needed
  because a dev user opting into `--profile mlops` while prod — which now always includes
  `mlflow-server` — is running would otherwise collide on the literal daemon-global name).
- OpenMetadata's six services (`openmetadata-mysql`, `openmetadata-elasticsearch`,
  `openmetadata-migrate`, `openmetadata-server`, `openmetadata-ingestion`) **keep their
  `container_name` and stay dev-only/opt-in in both stacks** — nothing at runtime depends on them
  (unlike `mlflow-server`), so they are out of scope for prod entirely and their names never collide
  since prod never starts them.
- Cosmetic: update the top-of-file comment banner to document both the dev and prod invocation
  forms.

## 3. Secrets & env templates

`.gitignore`: add a literal `.env.prod` line (not a wildcard like `.env.*`, which would also wrongly
ignore the committed `.env.example`/`.env.prod.example` templates).

`.env.example`: add `AIRFLOW_DB_PORT=5432`, `AIRFLOW_WEBSERVER_PORT=8080`, `MLFLOW_PORT=5000`,
`DBT_TARGET=dev`, `COMPOSE_PROFILES=` (empty — dev's mlflow/OpenMetadata stay opt-in via explicit
`--profile` flags, unchanged).

New `.env.prod.example`: same exact variable-name set as the updated `.env.example` (only values
differ) — `AIRFLOW_DB_PORT=5442`, `WAREHOUSE_DB_PORT=5443`, `AIRFLOW_WEBSERVER_PORT=8090` (not 8081
— see port-collision note above), `MLFLOW_PORT=5010`, `DBT_TARGET=prod`,
`COMPOSE_PROFILES=prod-core`, every secret var kept as the same `changeme`-style placeholder as
`.env.example` with a comment to generate independently, never copy from `.env`.

**`WAREHOUSE_DB_HOST`/`WAREHOUSE_DB_PORT` parity resolved:** `.env.example`'s existing tail section
(`rag/query.py host-side connection (run from host, not inside Docker)`) sets
`WAREHOUSE_DB_HOST=localhost` and `WAREHOUSE_DB_PORT=5433` — a host-side convenience pair for
running `rag/query.py` or manual `dbt` CLI commands directly from the host shell against the
container's *published* port, not forwarded into any container (`docker-compose.yml`'s
`x-airflow-common-env` forwards only `WAREHOUSE_DB_USER`/`PASSWORD`/`NAME`). For the variable-name
set to match exactly between the two templates, `.env.prod.example` carries the same pair, retargeted
at prod's mapping: `WAREHOUSE_DB_HOST=localhost`, `WAREHOUSE_DB_PORT=5443`. This is inert from
Compose's perspective (Compose reads `WAREHOUSE_DB_PORT` again for the port-mapping default, and
neither var reaches a container) and correct for a host-side script pointed at `.env.prod` — it
resolves to prod's actual published port.

## 4. Documentation

`README.md` gets: architecture table columns for dev-port/prod-port/env-var plus an `mlflow-server`
row; a new "Running dev and prod side by side" subsection with exact invocation commands and the
shared-bind-mount caveat from `Approach` above; a Quick Start "Prod stack (optional)" block; secrets
setup instructions for `.env.prod`; the `container_name` fix at line 190 with a prod-equivalent
example; Project Structure tree additions; a Key Design Decisions row for the shared-compose-file
approach; and prod-stack teardown commands.

## 5. Testing

New `tests/test_environment_separation.py`, mirroring `tests/test_dag_integrity.py` (`yaml.safe_load`
over `docker-compose.yml`, subprocess-based fresh-process imports for anything env-var-sensitive
rather than monkeypatch+reload, since `dags.dbt_pipeline_dag` may already be imported elsewhere in
the pytest session) and `tests/test_operational_defaults.py` (module-level `REPO_ROOT`, plain
`assert`, no custom helpers):

| Test | Covers |
|---|---|
| `DBT_TARGET` unset/`=prod` via clean subprocess import | `PROFILE_CONFIG.target_name` resolves to `"dev"` / `"prod"` |
| `profiles.yml` structure | has a `prod` key under `dbt_warehouse.outputs`; top-level `target` is the `env_var(...)` Jinja string |
| Compose port templating | `postgres_airflow`/`postgres_warehouse`/`airflow-webserver`/`mlflow-server` ports are `${VAR:-default}` strings, not literals |
| No `container_name` on core services | **exactly** `postgres_airflow`, `postgres_warehouse`, `airflow-init`, `airflow-webserver`, `airflow-scheduler` — enumerated explicitly, not "no service anywhere," since the six OpenMetadata services intentionally keep theirs (dev-only, never run in prod, so no collision risk) |
| `x-airflow-common.environment.DBT_TARGET` | `== "${DBT_TARGET:-dev}"` |
| `mlflow-server` profiles | list contains both `"mlops"` and `"prod-core"` |
| `.env.prod.example` / `.env.example` parity | regex-extracted `KEY=` name sets are exactly equal (regex must match `KEY=` with an empty value too, e.g. `SLACK_WEBHOOK_URL=`, `OPENAI_API_KEY=`) |
| `.gitignore` | contains a literal `.env.prod` line, no `.env.*` wildcard |

No live-Docker dependency for any of these — all pure static-file/subprocess-import tests, same
tier as the existing DAG-integrity suite.

---

## Explicitly out of scope

Real cloud deployment (this phase only removes local-only hardcoding). A second
`docker-compose.prod.yml` override file (rejected, see `Approach`). Engineering around the shared
host-bind-mount limitation (documented as an operational caveat instead). Generating real prod
secrets (user-created from the template). CI wiring for the prod stack (Phase 1's CI only exercises
the dev-shaped default env). Bringing OpenMetadata into prod's service scope (nothing depends on it
at runtime).
