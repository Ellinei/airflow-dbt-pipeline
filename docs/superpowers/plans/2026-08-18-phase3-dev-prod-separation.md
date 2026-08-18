# Phase 3: Dev/Prod Environment Separation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a second "prod" Docker Compose stack run simultaneously with the existing dev stack
via one shared, env-var-parameterized `docker-compose.yml` (project-name isolation, no second
compose file), and wire the one hardcoded dbt `target_name="dev"` to a `DBT_TARGET` env var so every
dbt invocation in a stack picks up the right target.

**Architecture:** `dbt_project/profiles.yml` gains a `prod` output block and an `env_var`-driven
top-level `target`; `dags/dbt_pipeline_dag.py`'s `ProfileConfig` reads the same `DBT_TARGET` env var.
`docker-compose.yml`'s five always-on core services plus `mlflow-server` get `${VAR:-default}` host
ports and lose their static `container_name:` (Compose auto-names per project instead); dev's
invocation stays exactly as-is (no `-p` flag, same implicit project name). `.env.prod.example`
mirrors `.env.example`'s variable-name set with prod's port/target/profile values. OpenMetadata's six
services are untouched — dev-only/opt-in in both stacks, nothing depends on them at runtime.

**Tech Stack:** Docker Compose (env-var interpolation, `COMPOSE_PROFILES`, project-name isolation),
dbt `env_var()` Jinja, Airflow 2.9.1 / Cosmos `ProfileConfig`. No new dependencies.

## Global Constraints

- Design spec: `docs/superpowers/specs/2026-08-18-phase3-dev-prod-separation-design.md` (read it for
  full rationale — this plan implements it).
- Python 3.12, Airflow 2.9.1 — same as Phase 0/1/2.
- **Two Compose mechanisms this plan relies on were already empirically verified directly against
  the running Docker install (read-only, no containers started) — do not re-verify them:**
  - Default Compose project name with no `-p` flag is `airflowdbtpipeline` (`docker compose config |
    grep '^name:'`), matching existing volume names — confirms dev's invocation must never change.
  - `COMPOSE_PROFILES` OR-activation: `COMPOSE_PROFILES=mlops docker compose config --services`
    already includes `mlflow-server` today; unset, it doesn't — confirms the `prod-core` profile
    mechanism Task 2 adds will work the same way.
- **Local Windows testing:** `apache-airflow`'s `operators.python` unconditionally does `import
  fcntl` (POSIX-only), so any test file that imports from `dags/*.py` fails to *collect* on native
  Windows (confirmed during Phase 1). Verify every pytest-running step below using a disposable Linux
  container instead, attached to the project's Docker network so it can reach `postgres_warehouse`:
  ```bash
  WINPATH=$(cygpath -m "$(pwd)")
  export MSYS_NO_PATHCONV=1
  docker run --rm --network airflowdbtpipeline_default \
    -v "${WINPATH}:/workspace" -w /workspace \
    -e WAREHOUSE_DB_HOST=postgres_warehouse -e WAREHOUSE_DB_PORT=5432 \
    -e WAREHOUSE_DB_USER=warehouse -e WAREHOUSE_DB_PASSWORD=warehouse -e WAREHOUSE_DB_NAME=warehouse \
    python:3.12-slim bash -c "
      pip install --no-cache-dir --constraint 'https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.12.txt' 'apache-airflow==2.9.1' -r requirements.txt &&
      pip install --no-cache-dir --no-deps 'dbt-postgres==1.8.0' 'pandas==2.1.4' &&
      pip install --no-cache-dir -r requirements-dev.txt &&
      ruff check . && pytest -v
    "
  ```
  (`postgres_warehouse` must already be running: `docker compose up -d postgres_warehouse` from the
  repo root, outside the container.)
- No live Docker-stack dependency for any test in this plan — all pure static-file (`yaml.safe_load`)
  or subprocess-import assertions, same tier as `tests/test_dag_integrity.py`.
- `dags/` has no `__init__.py` and is imported as a namespace package via `pythonpath = ["."]`
  (`pyproject.toml`) — `from dags.dbt_pipeline_dag import PROFILE_CONFIG` works the same way
  `tests/test_ingest_olist.py` already relies on.
- ruff config (`pyproject.toml`): `select = ["E", "F", "I"]`, line-length 100. Run `ruff check .`
  before every commit — do not commit with lint errors outstanding.
- **The design spec's Verification steps 3, 4, and 6 (starting both stacks concurrently, forcing the
  `mlflow-server` collision scenario, triggering a live `dbt_pipeline` run in prod) are explicitly
  NOT part of any task's TDD loop below.** They mutate real Docker state (containers, volumes, a live
  warehouse) on the user's machine and must only run after explicit check-in, per this session's
  guardrail on hard-to-reverse/shared-state actions. Task 5 lists them as manual follow-up steps, not
  something a task executes automatically.

---

### Task 1: dbt target wiring

**Files:**
- Modify: `dbt_project/profiles.yml`
- Modify: `dags/dbt_pipeline_dag.py`
- Create: `tests/test_environment_separation.py`

**Interfaces:**
- Produces: `dbt_project/profiles.yml`'s `prod` output block and `env_var`-driven top-level `target`;
  `dags/dbt_pipeline_dag.py`'s `PROFILE_CONFIG.target_name` reads `DBT_TARGET` — Task 2's
  `docker-compose.yml` change forwards that env var into containers.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_environment_separation.py`:
```python
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


def _target_name_in_subprocess(dbt_target: str | None) -> str:
    """Imports dags.dbt_pipeline_dag in a clean subprocess and returns
    PROFILE_CONFIG.target_name — a subprocess import (not monkeypatch +
    importlib.reload) because dags.dbt_pipeline_dag may already be imported
    elsewhere in the pytest session, same pattern as
    tests/test_dag_integrity.py's test_dags_import_with_container_realistic_syspath."""
    env = {**os.environ, "PYTHONPATH": str(REPO_ROOT)}
    if dbt_target is None:
        env.pop("DBT_TARGET", None)
    else:
        env["DBT_TARGET"] = dbt_target
    code = "from dags.dbt_pipeline_dag import PROFILE_CONFIG; print(PROFILE_CONFIG.target_name)"
    result = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def test_dbt_target_defaults_to_dev_when_unset():
    assert _target_name_in_subprocess(None) == "dev"


def test_dbt_target_reads_env_var_when_set():
    assert _target_name_in_subprocess("prod") == "prod"


def test_profiles_yml_has_prod_target_and_env_var_default():
    profiles = yaml.safe_load((REPO_ROOT / "dbt_project" / "profiles.yml").read_text())
    outputs = profiles["dbt_warehouse"]["outputs"]
    assert "prod" in outputs
    assert profiles["dbt_warehouse"]["target"] == "{{ env_var('DBT_TARGET', 'dev') }}"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_environment_separation.py -v
```
Expected: `test_dbt_target_reads_env_var_when_set` FAILS (`PROFILE_CONFIG.target_name` is still the
hardcoded literal `"dev"` regardless of `DBT_TARGET`); `test_profiles_yml_has_prod_target_and_env_var_default`
FAILS (no `prod` key exists yet; top-level `target` is still the plain string `"dev"`).
`test_dbt_target_defaults_to_dev_when_unset` passes already (existing hardcoded default happens to
match) — that's expected and fine, it's a defaults-lock-in test, not a regression check.

- [ ] **Step 3: Modify `dbt_project/profiles.yml`**

Current (lines 9-25):
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

Replace with:
```yaml
dbt_warehouse:
  target: "{{ env_var('DBT_TARGET', 'dev') }}"
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
    # Same env_var() names as dev — differentiation comes from which stack's
    # env vars are live at runtime (dev vs prod docker-compose invocation),
    # not from different variable names.
    prod:
      type: postgres
      host: "{{ env_var('WAREHOUSE_DB_HOST', 'postgres_warehouse') }}"
      port: "{{ env_var('WAREHOUSE_DB_PORT', '5432') | as_number }}"
      user: "{{ env_var('WAREHOUSE_DB_USER') }}"
      password: "{{ env_var('WAREHOUSE_DB_PASSWORD') }}"
      dbname: "{{ env_var('WAREHOUSE_DB_NAME') }}"
      schema: public
      threads: 4
      connect_timeout: 10
```

- [ ] **Step 4: Modify `dags/dbt_pipeline_dag.py`**

Current (lines 128-132):
```python
PROFILE_CONFIG = ProfileConfig(
    profile_name="dbt_warehouse",
    target_name="dev",
    profiles_yml_filepath=DBT_PROFILES_PATH / "profiles.yml",
)
```

Replace with:
```python
PROFILE_CONFIG = ProfileConfig(
    profile_name="dbt_warehouse",
    target_name=os.getenv("DBT_TARGET", "dev"),
    profiles_yml_filepath=DBT_PROFILES_PATH / "profiles.yml",
)
```
(`os` is already imported at the top of the file and used elsewhere, e.g.
`os.getenv("WAREHOUSE_DB_USER", "warehouse")` inside `ingest_olist`.)

- [ ] **Step 5: Run tests to verify they pass**

```bash
pytest tests/test_environment_separation.py -v
```
Expected: `3 passed`.

- [ ] **Step 6: Lint and commit**

```bash
ruff check .
git add dbt_project/profiles.yml dags/dbt_pipeline_dag.py tests/test_environment_separation.py
git commit -m "Wire DBT_TARGET env var through profiles.yml and dbt_pipeline_dag's ProfileConfig"
```

---

### Task 2: `docker-compose.yml` parameterization

**Files:**
- Modify: `docker-compose.yml`
- Modify: `tests/test_environment_separation.py`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `${VAR:-default}` host ports and no `container_name` on the five core services plus
  `mlflow-server`; `x-airflow-common-env.DBT_TARGET`; `mlflow-server`'s `["mlops", "prod-core"]`
  profiles — Task 3's `.env.prod.example` supplies the values that activate the prod side of all of
  these.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_environment_separation.py` (after the existing tests):
```python
def test_core_service_ports_are_templated():
    compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())
    services = compose["services"]
    assert services["postgres_airflow"]["ports"] == ["${AIRFLOW_DB_PORT:-5432}:5432"]
    assert services["postgres_warehouse"]["ports"] == ["${WAREHOUSE_DB_PORT:-5433}:5432"]
    assert services["airflow-webserver"]["ports"] == ["${AIRFLOW_WEBSERVER_PORT:-8080}:8080"]
    assert services["mlflow-server"]["ports"] == ["${MLFLOW_PORT:-5000}:5000"]


CORE_SERVICES_WITHOUT_CONTAINER_NAME = {
    "postgres_airflow",
    "postgres_warehouse",
    "airflow-init",
    "airflow-webserver",
    "airflow-scheduler",
}


def test_core_services_have_no_container_name():
    """Only these five (plus mlflow-server, checked separately) must lose
    container_name — OpenMetadata's six services intentionally keep theirs:
    they're dev-only/opt-in in both stacks and never run in prod, so no
    cross-project collision is possible."""
    compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())
    services = compose["services"]
    for service_name in CORE_SERVICES_WITHOUT_CONTAINER_NAME:
        assert "container_name" not in services[service_name], service_name


def test_dbt_target_forwarded_to_containers():
    compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())
    env = compose["x-airflow-common"]["environment"]
    assert env["DBT_TARGET"] == "${DBT_TARGET:-dev}"


def test_mlflow_server_has_both_profiles():
    compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())
    profiles = compose["services"]["mlflow-server"]["profiles"]
    assert set(profiles) == {"mlops", "prod-core"}
    assert "container_name" not in compose["services"]["mlflow-server"]
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_environment_separation.py -v
```
Expected: all 4 new tests FAIL — ports are still literal (`"5432:5432"` etc.), all five core
services plus `mlflow-server` still have `container_name`, `DBT_TARGET` isn't in
`x-airflow-common`'s environment, and `mlflow-server`'s `profiles` is still `["mlops"]` only.

- [ ] **Step 3: Modify `docker-compose.yml`**

`x-airflow-common-env` — add `DBT_TARGET` next to the existing `WAREHOUSE_DB_*` forwarding:
```yaml
    # Warehouse credentials forwarded so dbt profiles.yml can read them
    WAREHOUSE_DB_USER: "${WAREHOUSE_DB_USER}"
    WAREHOUSE_DB_PASSWORD: "${WAREHOUSE_DB_PASSWORD}"
    WAREHOUSE_DB_NAME: "${WAREHOUSE_DB_NAME}"
    DBT_TARGET: "${DBT_TARGET:-dev}"
```

`postgres_airflow` — remove `container_name: postgres_airflow`; port:
```yaml
    ports:
      - "${AIRFLOW_DB_PORT:-5432}:5432"
```

`postgres_warehouse` — remove `container_name: postgres_warehouse`; port:
```yaml
    ports:
      - "${WAREHOUSE_DB_PORT:-5433}:5432"          # Host 5433 → container 5432 (avoids clash with metadata DB)
```
**Do not** add `WAREHOUSE_DB_PORT` to `x-airflow-common-env` — it's not forwarded into any container
today (only USER/PASSWORD/NAME are), and `profiles.yml` already falls back to the container-internal
port `5432` when unset in-container. Forwarding it would make in-container dbt dial the *host*
mapping value instead of the real internal port, breaking every dbt connection in prod.

`airflow-init` — remove `container_name: airflow_init` only; its `command:` block is untouched.

`airflow-webserver` — remove `container_name: airflow_webserver`; port:
```yaml
    ports:
      - "${AIRFLOW_WEBSERVER_PORT:-8080}:8080"
```

`airflow-scheduler` — remove `container_name: airflow_scheduler` (no port mapping on this service).

`mlflow-server`:
```yaml
  mlflow-server:
    image: ghcr.io/mlflow/mlflow:v2.15.0
    profiles: ["mlops", "prod-core"]
    command: >
      mlflow server
      --backend-store-uri sqlite:////mlflow/mlflow.db
      --artifacts-destination /mlflow/artifacts
      --serve-artifacts
      --host 0.0.0.0
      --port 5000
    ports:
      - "${MLFLOW_PORT:-5000}:5000"
    volumes:
      - mlflow-data:/mlflow
    restart: unless-stopped
```
(`container_name: mlflow_server` removed; container-side `--port 5000` and the mapping's container
side stay untouched — only the host side is parameterized.)

Top-of-file comment banner — update to document both invocation forms:
```yaml
# ══════════════════════════════════════════════════════════════════════════════
#  Airflow + dbt + PostgreSQL — dev/prod stacks (Phase 3)
#  Dev  (default, unchanged):  docker compose up -d
#    Airflow UI → http://localhost:8080 | Warehouse → localhost:5433
#  Prod (side-by-side, via .env.prod):
#    docker compose -p airflow-dbt-prod --env-file .env.prod up -d
#    Airflow UI → http://localhost:8090 | Warehouse → localhost:5443
#    mlflow-server auto-starts (COMPOSE_PROFILES=prod-core in .env.prod).
#  OM Catalog  → http://localhost:8585  (admin / see .env)  [--profile catalog, dev only]
#  Credentials are generated per .env.example / .env.prod.example — see README.
# ══════════════════════════════════════════════════════════════════════════════
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_environment_separation.py -v
```
Expected: `7 passed` (the 3 from Task 1 plus these 4).

- [ ] **Step 5: Lint and commit**

```bash
ruff check .
git add docker-compose.yml tests/test_environment_separation.py
git commit -m "Parameterize docker-compose.yml ports and remove static container_name for prod isolation"
```

---

### Task 3: env templates + `.gitignore`

**Files:**
- Modify: `.env.example`
- Modify: `.gitignore`
- Create: `.env.prod.example`
- Modify: `tests/test_environment_separation.py`

**Interfaces:**
- Consumes: the variable names Task 2 wired into `docker-compose.yml`
  (`AIRFLOW_DB_PORT`/`AIRFLOW_WEBSERVER_PORT`/`MLFLOW_PORT`/`DBT_TARGET`/`COMPOSE_PROFILES`, plus the
  pre-existing `WAREHOUSE_DB_PORT`).

**`WAREHOUSE_DB_HOST`/`WAREHOUSE_DB_PORT` parity note** (see design spec §3 for full reasoning):
`.env.example` already defines these two under its "rag/query.py host-side connection" section as a
host-side convenience pair, not forwarded into any container. `.env.prod.example` carries the same
pair retargeted at prod's mapping (`WAREHOUSE_DB_PORT=5443`) so the variable-name sets match exactly
— this is inert from Compose's perspective and correct for a host-side script pointed at `.env.prod`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_environment_separation.py` (after the existing tests; add `import re` to the
imports at the top of the file):
```python
ENV_VAR_NAME_RE = re.compile(r"^([A-Z_][A-Z0-9_]*)=", re.MULTILINE)


def _env_var_names(path: Path) -> set[str]:
    return set(ENV_VAR_NAME_RE.findall(path.read_text()))


def test_env_prod_example_has_same_variable_names_as_env_example():
    dev_names = _env_var_names(REPO_ROOT / ".env.example")
    prod_names = _env_var_names(REPO_ROOT / ".env.prod.example")
    assert prod_names == dev_names


def test_gitignore_ignores_env_prod_literally():
    gitignore_lines = (REPO_ROOT / ".gitignore").read_text().splitlines()
    assert ".env.prod" in gitignore_lines
    assert not any(line.strip() == ".env.*" for line in gitignore_lines)
```
Remember to add `import re` alongside the existing `import os` / `import subprocess` / `import sys`
imports at the top of the file.

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_environment_separation.py -v
```
Expected: `test_env_prod_example_has_same_variable_names_as_env_example` errors
(`.env.prod.example` doesn't exist yet); `test_gitignore_ignores_env_prod_literally` FAILS (no
`.env.prod` line in `.gitignore` yet).

- [ ] **Step 3: Modify `.env.example`**

Insert a new section right after the top file header comment (before "Airflow metadata database"):
```
# ── Dev/prod stack selection ────────────────────────────────────────────────
# Host-side port mappings and the active dbt target. Defaults here match the
# stack's original hardcoded values — override only when running the prod
# stack via .env.prod (see .env.prod.example and README's "Running dev and
# prod side by side" section).
AIRFLOW_DB_PORT=5432
AIRFLOW_WEBSERVER_PORT=8080
MLFLOW_PORT=5000
DBT_TARGET=dev
# Comma-separated Compose profile names to auto-activate (OR'd with any
# --profile flag passed on the command line). Empty for dev — mlflow-server
# and OpenMetadata stay opt-in via explicit --profile flags.
COMPOSE_PROFILES=

```
Update the existing `WAREHOUSE_DB_PORT` line's context at the bottom of the file:
```
# ── rag/query.py host-side connection (run from host, not inside Docker) ───────
WAREHOUSE_DB_HOST=localhost
# Also drives docker-compose.yml's postgres_warehouse host port mapping
# (${WAREHOUSE_DB_PORT:-5433}:5432) — previously coincidentally matched
# without being wired together; now the same variable serves both purposes.
WAREHOUSE_DB_PORT=5433
```

- [ ] **Step 4: Create `.env.prod.example`**

```
# Copy this file to .env.prod and fill in independently-generated secrets —
# never copy values from .env. Used via:
#   docker compose -p airflow-dbt-prod --env-file .env.prod up -d
# See README.md's "Running dev and prod side by side" section.

# ── Dev/prod stack selection ────────────────────────────────────────────────
AIRFLOW_DB_PORT=5442
AIRFLOW_WEBSERVER_PORT=8090
MLFLOW_PORT=5010
DBT_TARGET=prod
# Always activates mlflow-server in the prod stack (OR'd with any --profile
# flag) — no extra --profile flag needed on the command line.
COMPOSE_PROFILES=prod-core

# ── Airflow metadata database ──────────────────────────────────────────────────
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=changeme   # generate an independent secret — do not copy from .env
AIRFLOW_DB_NAME=airflow

# ── Data warehouse database ────────────────────────────────────────────────────
WAREHOUSE_DB_USER=warehouse
WAREHOUSE_DB_PASSWORD=changeme   # generate an independent secret — do not copy from .env
WAREHOUSE_DB_NAME=warehouse

# ── Airflow UID (keeps file ownership consistent on Linux/Mac; ignored on Win) ─
AIRFLOW_UID=50000

# ── Alerting ───────────────────────────────────────────────────────────────────
SLACK_WEBHOOK_URL=

# ── Cognitive layer ──────────────────────────────────────────────────────────
OPENAI_API_KEY=

# ── Airflow encryption / auth ───────────────────────────────────────────────────
AIRFLOW_FERNET_KEY=changeme   # generate an independent secret — do not copy from .env
AIRFLOW_ADMIN_PASSWORD=changeme   # generate an independent secret — do not copy from .env

# ── OpenMetadata (--profile catalog) ────────────────────────────────────────────
OPENMETADATA_MYSQL_ROOT_PASSWORD=changeme   # generate an independent secret — do not copy from .env
OPENMETADATA_DB_PASSWORD=changeme   # generate an independent secret — do not copy from .env
OPENMETADATA_JWT_TOKEN=changeme   # generate an independent token — do not copy from .env

# ── Governance demo users (governance/setup_roles.sql) ─────────────────────────
GOVERNANCE_ENGINEER_PASSWORD=changeme   # generate an independent secret — do not copy from .env
GOVERNANCE_ANALYST_PASSWORD=changeme   # generate an independent secret — do not copy from .env

# ── rag/query.py host-side connection (run from host, not inside Docker) ───────
WAREHOUSE_DB_HOST=localhost
WAREHOUSE_DB_PORT=5443
```

- [ ] **Step 5: Modify `.gitignore`**

Current (line 2):
```
# ── Credentials — NEVER commit these ──────────────────────────────────────────
.env
```

Replace with:
```
# ── Credentials — NEVER commit these ──────────────────────────────────────────
.env
.env.prod
```

- [ ] **Step 6: Run tests to verify they pass**

```bash
pytest tests/test_environment_separation.py -v
```
Expected: `9 passed` (all 7 from Tasks 1-2 plus these 2).

- [ ] **Step 7: Lint and commit**

```bash
ruff check .
git add .env.example .env.prod.example .gitignore tests/test_environment_separation.py
git commit -m "Add .env.prod.example template and gitignore entry for prod secrets"
```

---

### Task 4: README updates

**Files:**
- Modify: `README.md`

**Interfaces:** none — documentation only, no tests (nothing here is behavior to verify by
assertion; this task's own "verification" is a careful re-read against Tasks 1-3's actual changes).

- [ ] **Step 1: Architecture section's service/port table**

Add Dev-port / Prod-port / env-var columns to the existing table, and add an `mlflow-server` row
(currently missing — confirm by reading the table first, since its exact current columns/rows must
be read before editing, not assumed).

- [ ] **Step 2: Fix the broken `container_name`-dependent command**

Current (`README.md:190`):
```
docker exec -it airflow_scheduler bash
```
Replace with:
```
docker compose exec airflow-scheduler bash
```
Add a prod-equivalent example immediately alongside it:
```
docker compose -p airflow-dbt-prod --env-file .env.prod exec airflow-scheduler bash
```

- [ ] **Step 3: New "Running dev and prod side by side" subsection**

Include the exact invocation commands:
```bash
# Dev (unchanged)
docker compose up -d

# Prod (side by side)
docker compose -p airflow-dbt-prod --env-file .env.prod up -d
```
Note that `mlflow-server` auto-starts in prod via `COMPOSE_PROFILES=prod-core` in `.env.prod` — no
extra `--profile` flag needed. Add a callout: host bind mounts (`./dags`, `./logs`, `./dbt_project`,
`./data`) are shared between the two stacks — only containers/volumes/ports are isolated — so avoid
triggering `dbt_pipeline` in both stacks at the same moment (races on the shared
`dbt_project/target/`/`dbt_packages/` directories).

- [ ] **Step 4: Quick Start — "Prod stack (optional, runs alongside dev)" block**

Same commands as Step 3, placed in context with the existing Quick Start flow.

- [ ] **Step 5: Secrets & Configuration section**

Add the `.env.prod.example` → `.env.prod` instruction: copy, fill independent secrets (never copy
from `.env`), gitignored like `.env`.

- [ ] **Step 6: Project Structure tree**

Add `.env.prod` / `.env.prod.example` lines.

- [ ] **Step 7: Key Design Decisions table**

Add a row for the shared-compose-file / project-name-isolation approach, naming the shared-bind-mount
trade-off explicitly (same wording as the design spec's `Approach` section).

- [ ] **Step 8: Stopping/Resetting section**

Add the prod-stack equivalents of `docker compose down` / `down -v`:
```bash
docker compose -p airflow-dbt-prod --env-file .env.prod down
docker compose -p airflow-dbt-prod --env-file .env.prod down -v
```

- [ ] **Step 9: Commit**

```bash
git add README.md
git commit -m "Document dev/prod side-by-side stack setup in README"
```

---

### Task 5: Full regression + safe verification

**Files:** none — verification only.

**Interfaces:** none.

- [ ] **Step 1: Run the full suite together**

Using the Linux-container pattern from Global Constraints (with `postgres_warehouse` already
running):
```bash
ruff check .
pytest -v
```
Expected: all tests pass — including all 9 in `tests/test_environment_separation.py` alongside the
pre-existing `tests/test_dag_integrity.py`, `tests/test_operational_defaults.py`,
`tests/test_ingest_olist.py`, `tests/test_rag_index.py`, `tests/test_dbt_build.py`.

- [ ] **Step 2: Confirm no leftover hardcoded target**

```bash
grep -rn 'target_name="dev"' dags/
```
Expected: no matches.

- [ ] **Step 3: `.env.prod` from template, confirm `COMPOSE_PROFILES` reads from `--env-file`**

Requires a real `.env.prod` created from the template (not committed). After creating one:
```bash
docker compose -p airflow-dbt-prod --env-file .env.prod config --services
```
Expected: `mlflow-server` is listed with no `--profile` flag on the command line — confirms
`COMPOSE_PROFILES` is read correctly from `--env-file`, not just the shell.

- [ ] **Step 4: `git status` confirms `.env.prod` stays untracked**

```bash
git status
```
Expected: `.env.prod` does not appear (or appears only as untracked and ignored) — confirms
`.gitignore`'s new entry works.

- [ ] **Step 5 — requires explicit user check-in before running (starts real containers / mutates
  shared Docker state on this machine):**

The design spec's Verification steps 3, 4, and 6 are **not** run automatically as part of this plan.
Ask the user before running any of them:
1. Start both stacks concurrently (`docker compose up -d` then `docker compose -p airflow-dbt-prod
   --env-file .env.prod up -d`); confirm via `docker ps` and `docker volume ls` that the two stacks
   have fully disjoint name sets, with no "address already in use" errors.
2. Force the exact collision scenario `container_name` removal prevents: with prod already up, run
   `docker compose --profile mlops up -d mlflow-server` for dev — two separate `mlflow-server`
   containers must coexist (dev on port 5000, prod on port 5010).
3. `docker compose -p airflow-dbt-prod --env-file .env.prod exec airflow-scheduler python -c "import
   os; print(os.getenv('DBT_TARGET'))"` → expect `prod`.
4. Trigger `dbt_pipeline` in the **prod stack only** (not dev, at the same time — shared
   `dbt_project/target/`/`dbt_packages/` bind mount) and confirm its task logs show the dbt run using
   the `prod` target.
5. Tear both stacks down afterward (`docker compose down` / `docker compose -p airflow-dbt-prod
   --env-file .env.prod down`).
