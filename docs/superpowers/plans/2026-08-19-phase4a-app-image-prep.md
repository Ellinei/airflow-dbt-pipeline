# Phase 4a: App & Image Prep for Cloud Deployment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the two real gaps Phase 4's design pass found in the app code — a hardcoded
`postgres_warehouse:5432` hostname and a Dockerfile that never bakes `dags/`/`dbt_project/` into the
image — and add the Olist S3 fallback + `apache-airflow-providers-amazon` dependency the AWS
deployment needs, with zero behavior change to the existing local dev/prod Compose stacks. This is
the half of Phase 4 that's pure application code, testable entirely with pytest, no AWS account
required. Phase 4b (separate plan) covers the Terraform infrastructure, CI/CD deploy workflow, and
docs this code enables.

**Architecture:** `dags/dbt_pipeline_dag.py`'s `ingest_olist` task gets two module-level helpers
pulled out of its body — `_warehouse_engine_url()` (host/port now read from env, same fallback
defaults) and `_ensure_olist_data_available()` / `_download_olist_from_s3()` (a `boto3`-based S3
pre-download gated on a new `OLIST_S3_BUCKET` env var, unset in both local Compose stacks) — following
the same "pull the logic out of the `@task` closure so it's independently unit-testable" pattern
`_ingest_olist_files` already established in Phase 1. The `Dockerfile` gains two `COPY --chown`
layers + a `dbt deps` build step so the image is self-contained (ECS Fargate has no bind-mount
equivalent). `requirements.txt` gains one new exact-pinned package.

**Tech Stack:** Python 3.12, Airflow 2.9.1, `boto3` (via `apache-airflow-providers-amazon`), pytest,
`unittest.mock`. No new local infrastructure.

**Spec:** `docs/superpowers/specs/2026-08-18-phase4-cloud-deployment-design.md` §1, §2, §3, §8
(dependency only), §13 rows 1-5 — read it for full rationale; this plan implements those sections.

## Global Constraints

- Python 3.12, Airflow 2.9.1, dbt-core 1.8.0, astronomer-cosmos 1.4.3 — same as Phases 0-3.
- **Reuse Phase 3's parameterization; no new dbt target, no new `DBT_TARGET` value.** Every new env
  var this plan introduces (`WAREHOUSE_DB_HOST`, `WAREHOUSE_DB_PORT` — already read by
  `profiles.yml`, just not yet by `ingest_olist`; `OLIST_S3_BUCKET`) is **unset in both
  `.env.example`/`.env.prod.example`**, so local dev/prod behavior stays byte-for-byte unchanged.
  Do not add these to `.env.example`/`.env.prod.example` or `docker-compose.yml` — they're
  AWS-deployment-only and get set directly in Phase 4b's ECS task definitions.
- **`_ingest_olist_files` (in `dags/dbt_pipeline_dag.py`) stays untouched** — preserves Phase 1's
  existing unit-test surface (`tests/test_ingest_olist.py`). All new logic lives in new module-level
  functions alongside it, not inside it.
- **Three facts verified empirically this session — do not re-derive, just use them:**
  - `apache-airflow-providers-amazon` resolves cleanly against this project's existing pinned
    `requirements.txt` + the `constraints-2.9.1/constraints-3.12.txt` constraints file, in ~2.5
    minutes with **zero pip resolver backtracking**. It resolves to **`apache-airflow-providers-amazon==8.20.0`**
    (pulling in `boto3==1.34.69`, `botocore==1.34.69`); `sqlalchemy` stays pinned at `1.4.52`
    (required — see the `Dockerfile`'s own comments on why). Verified via a throwaway
    `python:3.12-slim` container running the exact joint install command the Dockerfile uses, plus
    this one new package appended.
  - `dbt deps --project-dir dbt_project --profiles-dir dbt_project` **succeeds with exit 0 with
    `WAREHOUSE_DB_USER`/`WAREHOUSE_DB_PASSWORD`/`WAREHOUSE_DB_NAME` all unset** — it does not render
    `profiles.yml`'s target-specific `env_var()` calls (which have no defaults for those three keys).
    Verified empirically in a throwaway container against the real `dbt_project/`. **This means the
    Dockerfile's new `RUN dbt deps ...` step needs no throwaway build `ARG`s** — the spec flagged this
    as unverified; it's now closed.
  - Docker's `COPY` instruction (no `--chown`) leaves copied files/directories owned `root:root`,
    mode `rwxr-xr-x`. The `airflow` user (uid 50000, gid 0 — `docker-compose.yml`'s
    `user: "${AIRFLOW_UID:-50000}:0"`) can **read** such a directory (world execute+read bit) but
    **cannot write into it** (group-`root` only gets `r-x`, not `rwx`, and airflow isn't the owner).
    Verified empirically with a throwaway image (`touch` inside a plain-`COPY`'d directory as the
    `airflow` user fails with `Permission denied`; the same `touch` inside a `--chown=airflow:0`'d
    directory succeeds). Since Task 3's `RUN dbt deps` step must **write** `dbt_packages/` (and
    `logs/`, `.user.yml`) into the copied `dbt_project/` directory while running as the `airflow`
    user, **both `COPY` lines in Task 3 must use `--chown=airflow:0`** — this is a correctness
    requirement, not a defensive nicety.
- **Local Windows testing:** `apache-airflow`'s `operators.python` unconditionally does `import
  fcntl` (POSIX-only), so any test file that imports from `dags/*.py` fails to *collect* on native
  Windows. Every "Run: `pytest ...`" instruction below means: substitute that command into this
  disposable-Linux-container recipe (same one Phase 3's plan used), run from the repo root:
  ```bash
  WINPATH=$(cygpath -m "$(pwd)")
  export MSYS_NO_PATHCONV=1
  docker run --rm \
    -v "${WINPATH}:/workspace" -w /workspace \
    python:3.12-slim bash -c "
      pip install --no-cache-dir --constraint 'https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.12.txt' 'apache-airflow==2.9.1' -r requirements.txt &&
      pip install --no-cache-dir --no-deps 'dbt-postgres==1.8.0' 'pandas==2.1.4' &&
      pip install --no-cache-dir -r requirements-dev.txt &&
      ruff check . && <pytest command here>
    "
  ```
  **Exception:** any step whose test file has no module-scope `dags.*` import at that point in the
  sequence can run bare on Windows — called out explicitly per-step below. This window closes after
  Task 4: Task 4 adds a module-scope `from dags.dbt_pipeline_dag import ...` to
  `tests/test_cloud_deployment.py`, so pytest must import the whole file (triggering the `fcntl`
  failure on Windows) to collect *any* test in it from that point on — including Task 1's and Task
  3's plain-text tests, if re-run later. Tasks 1 and 3's "runs natively on Windows" instructions are
  only accurate when executed in this plan's order, before Task 4 lands; don't generalize them to "this
  test file is Windows-safe" once the whole plan is done.
  No live Postgres or Docker network is needed for *any* test in this plan (unlike Phase 3's plan,
  which needed `postgres_warehouse` running for other suites) — Task 5's full-suite regression run is
  the one exception, and it says so explicitly.
- ruff config (`pyproject.toml`): `select = ["E", "F", "I"]`, line-length 100. Run `ruff check .`
  before every commit — do not commit with lint errors outstanding.
- **Nothing in this plan touches AWS or costs money** — it's local file edits + local Docker builds
  only. No check-in gate needed beyond normal review.

---

### Task 1: Pin `apache-airflow-providers-amazon`

**Files:**
- Modify: `requirements.txt`
- Create: `tests/test_cloud_deployment.py`

**Interfaces:**
- Produces: `boto3`/`botocore` become available as transitive imports for Task 4's
  `_download_olist_from_s3`, and `apache-airflow-providers-amazon` becomes available for Phase 4b's
  remote task logging (design spec §8) — not configured in this plan, just installed.

- [ ] **Step 1: Write the failing test**

Create `tests/test_cloud_deployment.py`:
```python
from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_requirements_pins_amazon_provider():
    requirements = (REPO_ROOT / "requirements.txt").read_text()
    assert "apache-airflow-providers-amazon==8.20.0" in requirements
```

- [ ] **Step 2: Run test to verify it fails**

This test does no `dags.*` import — plain `Path.read_text()` — so it runs natively on Windows:
```bash
pytest tests/test_cloud_deployment.py -v
```
Expected: FAIL (`apache-airflow-providers-amazon==8.20.0` not yet in `requirements.txt`).

- [ ] **Step 3: Modify `requirements.txt`**

Current (lines 23-28):
```
psycopg2-binary==2.9.9
dbt-core==1.8.0
astronomer-cosmos==1.4.3
mlflow==2.22.5
scikit-learn==1.9.0
openai==1.25.0
```

Replace with:
```
psycopg2-binary==2.9.9
dbt-core==1.8.0
astronomer-cosmos==1.4.3
mlflow==2.22.5
scikit-learn==1.9.0
openai==1.25.0
apache-airflow-providers-amazon==8.20.0
```

Also update the file's header comment (lines 13-17), which currently reads:
```
# dbt-postgres and pandas are NOT listed here — they're installed separately
# in the Dockerfile with --no-deps (their dependencies are already satisfied
# by the packages above; a joint resolve of all of them together is what
# caused the original backtracking this file replaces). Their exact pinned
# versions (dbt-postgres==1.8.0, pandas==2.1.4) live inline in the Dockerfile.
```
Append one sentence directly after it:
```
#
# apache-airflow-providers-amazon (added Phase 4) resolves cleanly in this
# joint install — verified empirically, ~2.5 min, no backtracking — because
# its only meaningful new dependencies (boto3/botocore) have no version
# conflicts with anything else pinned here or in Airflow's own constraints.
```

- [ ] **Step 4: Run test to verify it passes**

```bash
pytest tests/test_cloud_deployment.py -v
```
Expected: `1 passed`.

- [ ] **Step 5: Lint and commit**

```bash
ruff check .
git add requirements.txt tests/test_cloud_deployment.py
git commit -m "Pin apache-airflow-providers-amazon for Phase 4 AWS deployment"
```

---

### Task 2: Fix the hardcoded warehouse hostname

**Files:**
- Modify: `dags/dbt_pipeline_dag.py`
- Modify: `tests/test_cloud_deployment.py`

**Interfaces:**
- Produces: module-level `_warehouse_engine_url() -> str` in `dags/dbt_pipeline_dag.py`, callable
  with no arguments, reads `WAREHOUSE_DB_USER`/`WAREHOUSE_DB_PASSWORD`/`WAREHOUSE_DB_NAME`/
  `WAREHOUSE_DB_HOST`/`WAREHOUSE_DB_PORT` from the environment. Task 4 calls this from `ingest_olist`
  unchanged.

- [ ] **Step 1: Write the failing tests**

Add the new imports to the top of `tests/test_cloud_deployment.py` (alongside the existing `from
__future__ import annotations` / `from pathlib import Path` block from Task 1 — do not create a
second `REPO_ROOT`, it already exists):
```python
import os
import subprocess
import sys
```

Then append the test code:
```python
def _warehouse_engine_url_in_subprocess(env_overrides: dict[str, str]) -> str:
    """Imports dags.dbt_pipeline_dag in a clean subprocess and calls
    _warehouse_engine_url() — same pattern as
    tests/test_environment_separation.py's _target_name_in_subprocess, needed
    because dags.dbt_pipeline_dag may already be imported elsewhere in the
    pytest session."""
    env = {**os.environ, "PYTHONPATH": str(REPO_ROOT)}
    for key in ("WAREHOUSE_DB_HOST", "WAREHOUSE_DB_PORT"):
        env.pop(key, None)
    env.update(env_overrides)
    code = "from dags.dbt_pipeline_dag import _warehouse_engine_url; print(_warehouse_engine_url())"
    result = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip().split("\n")[-1]


def test_warehouse_engine_url_falls_back_to_compose_service_name_when_unset():
    url = _warehouse_engine_url_in_subprocess({})
    assert "@postgres_warehouse:5432/" in url


def test_warehouse_engine_url_uses_rds_host_and_port_when_set():
    url = _warehouse_engine_url_in_subprocess({
        "WAREHOUSE_DB_HOST": "my-rds-endpoint.rds.amazonaws.com",
        "WAREHOUSE_DB_PORT": "5433",
    })
    assert "@my-rds-endpoint.rds.amazonaws.com:5433/" in url
```

- [ ] **Step 2: Run tests to verify they fail**

These import `dags.dbt_pipeline_dag` — use the Linux container recipe from Global Constraints,
substituting:
```bash
pytest tests/test_cloud_deployment.py -v
```
Expected: both new tests FAIL with `ImportError: cannot import name '_warehouse_engine_url'`
(`test_requirements_pins_amazon_provider` still passes).

- [ ] **Step 3: Modify `dags/dbt_pipeline_dag.py`**

Current (lines 145-171, inside the `dbt_pipeline()` DAG function):
```python
def dbt_pipeline() -> None:

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

Replace with:
```python
def dbt_pipeline() -> None:

    # ── Step 0: ingest the real-world Olist dataset into the raw schema ───────
    @task
    def ingest_olist() -> dict:
        """Thin Airflow wrapper — see _ingest_olist_files for the actual
        loading logic (module-level, independently unit-tested)."""
        import sqlalchemy

        engine = sqlalchemy.create_engine(_warehouse_engine_url())
        return _ingest_olist_files(engine, OLIST_DATA_DIR, OLIST_FILES)

    ingest = ingest_olist()
```

Then add the new module-level function directly above `_ingest_olist_files` (before line 73's
`def _ingest_olist_files(...)`):
```python
def _warehouse_engine_url() -> str:
    """Builds the SQLAlchemy engine URL for the warehouse DB from env vars,
    with the same postgres_warehouse:5432 fallback Compose has always used
    locally, so dev/prod behavior is unchanged. Pulled out of ingest_olist so
    it's testable via a clean-subprocess import without executing the task
    through Airflow — same rationale as _ingest_olist_files below."""
    db_user = os.getenv("WAREHOUSE_DB_USER", "warehouse")
    db_password = os.getenv("WAREHOUSE_DB_PASSWORD", "warehouse")
    db_name = os.getenv("WAREHOUSE_DB_NAME", "warehouse")
    db_host = os.getenv("WAREHOUSE_DB_HOST", "postgres_warehouse")
    db_port = os.getenv("WAREHOUSE_DB_PORT", "5432")
    return f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"


def _ingest_olist_files(engine, data_dir: Path, files_map: dict[str, str]) -> dict[str, int]:
```
(`os` is already imported at the top of the file.)

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_cloud_deployment.py -v
```
Expected: `3 passed`.

- [ ] **Step 5: Run the full existing suite to confirm no regression**

```bash
pytest tests/test_ingest_olist.py tests/test_dag_integrity.py -v
```
Expected: all pass — `_ingest_olist_files` is untouched, and `ingest_olist`'s only change is where
the engine URL comes from.

- [ ] **Step 6: Lint and commit**

```bash
ruff check .
git add dags/dbt_pipeline_dag.py tests/test_cloud_deployment.py
git commit -m "Fix hardcoded warehouse hostname/port in ingest_olist"
```

---

### Task 3: Bake `dags/` and `dbt_project/` into the image

**Files:**
- Modify: `Dockerfile`
- Modify: `tests/test_cloud_deployment.py`

**Interfaces:**
- Produces: no new Python interface — the image itself now carries `/opt/airflow/dags` and
  `/opt/airflow/dbt_project` with `dbt_packages/` pre-resolved. Phase 4b's ECS task definitions
  consume this image directly (no bind mounts, unlike local Compose).

- [ ] **Step 1: Write the failing test**

Append to `tests/test_cloud_deployment.py`:
```python
import re


def test_dockerfile_bakes_dags_and_dbt_project():
    dockerfile = (REPO_ROOT / "Dockerfile").read_text()
    assert "COPY --chown=airflow:0 dags/ /opt/airflow/dags/" in dockerfile
    assert "COPY --chown=airflow:0 dbt_project/ /opt/airflow/dbt_project/" in dockerfile
    assert re.search(r"^RUN .*dbt deps", dockerfile, re.MULTILINE)
    assert "COPY plugins/" not in dockerfile
```

- [ ] **Step 2: Run test to verify it fails**

No `dags.*` import — plain `Path.read_text()` — runs natively on Windows:
```bash
pytest tests/test_cloud_deployment.py::test_dockerfile_bakes_dags_and_dbt_project -v
```
Expected: FAIL (none of these lines exist yet).

- [ ] **Step 3: Modify `Dockerfile`**

Current (lines 74-78, the file's last two lines plus the blank line before them):
```dockerfile
# Kept as its own layer (not chained with `&&` above) so this line is the
# only one that invalidates on changes to the pandas pin — the heavy install
# above stays cache-identical to prior builds.
RUN pip install --no-cache-dir --no-deps "pandas==2.1.4"
```

Replace with:
```dockerfile
# Kept as its own layer (not chained with `&&` above) so this line is the
# only one that invalidates on changes to the pandas pin — the heavy install
# above stays cache-identical to prior builds.
RUN pip install --no-cache-dir --no-deps "pandas==2.1.4"

# ── Bake application code into the image (Phase 4) ────────────────────────────
# Locally, ./dags and ./dbt_project reach containers via docker-compose.yml
# bind mounts, which overlay these baked copies at container start — dev/prod
# behavior is unchanged. ECS Fargate has no bind-mount equivalent, so the
# image itself must carry the code (see Phase 4 design spec §2).
#
# --chown=airflow:0 is required, not optional: plain COPY leaves files owned
# root:root, mode rwxr-xr-x. The airflow user (uid 50000, gid 0) can read
# such a directory but cannot write into it — and the RUN below must create
# dbt_packages/ inside dbt_project/ while running as airflow. Verified
# empirically (a plain-COPY'd directory rejects `touch` as airflow with
# Permission denied; a --chown'd one accepts it).
COPY --chown=airflow:0 dags/ /opt/airflow/dags/
# dbt_project/ must land as a sibling of dags/ under /opt/airflow —
# dbt_pipeline_dag.py computes DBT_PROJECT_PATH as
# Path(__file__).resolve().parent.parent / "dbt_project", i.e.
# /opt/airflow/dags/../dbt_project = /opt/airflow/dbt_project.
COPY --chown=airflow:0 dbt_project/ /opt/airflow/dbt_project/
# Resolves dbt_packages/ at build time so Cosmos's LoadMode.DBT_LS (which
# shells out to `dbt ls` at DAG-parse time) doesn't fail silently on a fresh
# Fargate task — mirrors what the one-shot airflow-init Compose service does
# locally. No throwaway WAREHOUSE_DB_* build ARGs needed: verified
# empirically that `dbt deps` succeeds without them (it doesn't render
# profiles.yml's target-specific env_var() calls).
RUN dbt deps --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project

# plugins/ is deliberately NOT copied — it's .gitignore'd and empty on a
# fresh checkout; COPY from a path absent in the build context fails the
# build outright. The base image's own empty /opt/airflow/plugins is
# sufficient since nothing in this project populates it.
```

- [ ] **Step 4: Run test to verify it passes**

```bash
pytest tests/test_cloud_deployment.py::test_dockerfile_bakes_dags_and_dbt_project -v
```
Expected: `1 passed`.

- [ ] **Step 5: Build the image and verify the bake + permissions empirically**

```bash
docker compose build
export MSYS_NO_PATHCONV=1
docker run --rm --entrypoint ls airflow-dbt-pipeline:latest -la /opt/airflow/dbt_project
docker run --rm --entrypoint sh airflow-dbt-pipeline:latest -c "test -d /opt/airflow/dbt_project/dbt_packages && echo DBT_PACKAGES_OK"
docker run --rm --entrypoint sh airflow-dbt-pipeline:latest -c "touch /opt/airflow/dbt_project/test_write && echo WRITE_OK"
```
Expected: `dbt_project` owned `airflow root`; `DBT_PACKAGES_OK` printed; `WRITE_OK` printed (proves the
`--chown` fix actually works against the real build, not just the throwaway spike image).

- [ ] **Step 6: Lint and commit**

```bash
ruff check .
git add Dockerfile tests/test_cloud_deployment.py
git commit -m "Bake dags/ and dbt_project/ into the image for ECS Fargate"
```

---

### Task 4: Olist S3 fallback

**Files:**
- Modify: `dags/dbt_pipeline_dag.py`
- Modify: `tests/test_cloud_deployment.py`

**Interfaces:**
- Consumes: `OLIST_FILES` (module-level dict, already exists), `boto3` (from Task 1's dependency).
- Produces: module-level `_ensure_olist_data_available(data_dir: Path, files_map: dict[str, str],
  bucket: str | None) -> None` and `_download_olist_from_s3(bucket: str, prefix: str, dest: Path) ->
  None` in `dags/dbt_pipeline_dag.py`. `ingest_olist` calls `_ensure_olist_data_available` before
  building the engine.

- [ ] **Step 1: Write the failing tests**

Add two new imports to the top of `tests/test_cloud_deployment.py`, alongside Task 1's imports (Task
2's `_warehouse_engine_url` is only ever invoked inside a subprocess code string, so it never needed a
module-scope import here — this is the first one in this file):
```python
from unittest.mock import MagicMock, patch

from dags.dbt_pipeline_dag import OLIST_FILES, _ensure_olist_data_available
```
Use this single module-scope import in all three test functions below — do not re-import inside each
test body.

Then append the test code:
```python
def test_ensure_olist_data_skips_download_when_files_present_and_bucket_set(tmp_path):
    for filename in OLIST_FILES:
        (tmp_path / filename).write_text("stub")
    with patch("boto3.client") as mock_client:
        _ensure_olist_data_available(tmp_path, OLIST_FILES, "some-bucket")
    mock_client.assert_not_called()


def test_ensure_olist_data_skips_download_when_bucket_unset(tmp_path):
    with patch("boto3.client") as mock_client:
        _ensure_olist_data_available(tmp_path, OLIST_FILES, None)
    mock_client.assert_not_called()


def test_ensure_olist_data_downloads_when_files_missing_and_bucket_set(tmp_path):
    mock_s3 = MagicMock()
    with patch("boto3.client", return_value=mock_s3) as mock_client:
        _ensure_olist_data_available(tmp_path, OLIST_FILES, "some-bucket")
    mock_client.assert_called_once_with("s3")
    assert mock_s3.download_file.call_count == len(OLIST_FILES)
    called_keys = {call.args[1] for call in mock_s3.download_file.call_args_list}
    assert called_keys == {f"olist-raw/{f}" for f in OLIST_FILES}
    called_dests = {call.args[2] for call in mock_s3.download_file.call_args_list}
    assert called_dests == {str(tmp_path / f) for f in OLIST_FILES}
```

- [ ] **Step 2: Run tests to verify they fail**

These import `dags.dbt_pipeline_dag` — use the Linux container recipe, substituting:
```bash
pytest tests/test_cloud_deployment.py -v
```
Expected: the three new tests FAIL with `ImportError: cannot import name '_ensure_olist_data_available'`.

- [ ] **Step 3: Modify `dags/dbt_pipeline_dag.py`**

Add these two module-level functions directly above `_warehouse_engine_url` (which Task 2 placed
before `_ingest_olist_files`):
```python
def _download_olist_from_s3(bucket: str, prefix: str, dest: Path) -> None:
    """Pulls each Olist CSV from s3://{bucket}/{prefix} into dest (creating
    it if needed) via boto3. ECS Fargate's ephemeral storage has no
    persistent Olist data between task starts — see Phase 4 design spec §3."""
    import boto3

    dest.mkdir(parents=True, exist_ok=True)
    client = boto3.client("s3")
    for filename in OLIST_FILES:
        client.download_file(bucket, f"{prefix}{filename}", str(dest / filename))


def _ensure_olist_data_available(data_dir: Path, files_map: dict[str, str], bucket: str | None) -> None:
    """No-op when files are already present in data_dir, or when no S3
    bucket is configured — which is always true in local dev/prod, where
    OLIST_S3_BUCKET is unset, so local behavior is unchanged. Downloads all
    files from S3 only when at least one is missing and a bucket is set."""
    if bucket and not all((data_dir / f).exists() for f in files_map):
        _download_olist_from_s3(bucket=bucket, prefix="olist-raw/", dest=data_dir)


def _warehouse_engine_url() -> str:
```
(This inserts the two new functions immediately before the existing `_warehouse_engine_url` def
line from Task 2 — do not duplicate that line, just add the new functions above it.)

Then update `ingest_olist`. Current (from Task 2's result):
```python
    @task
    def ingest_olist() -> dict:
        """Thin Airflow wrapper — see _ingest_olist_files for the actual
        loading logic (module-level, independently unit-tested)."""
        import sqlalchemy

        engine = sqlalchemy.create_engine(_warehouse_engine_url())
        return _ingest_olist_files(engine, OLIST_DATA_DIR, OLIST_FILES)
```

Replace with:
```python
    @task
    def ingest_olist() -> dict:
        """Thin Airflow wrapper — see _ingest_olist_files for the actual
        loading logic (module-level, independently unit-tested)."""
        import sqlalchemy

        _ensure_olist_data_available(OLIST_DATA_DIR, OLIST_FILES, os.getenv("OLIST_S3_BUCKET"))
        engine = sqlalchemy.create_engine(_warehouse_engine_url())
        return _ingest_olist_files(engine, OLIST_DATA_DIR, OLIST_FILES)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_cloud_deployment.py -v
```
Expected: `7 passed` (Task 1: 1, Task 2: 2, Task 3: 1, Task 4: 3 — all of Tasks 1-4's tests in this
file).

- [ ] **Step 5: Run the full existing suite to confirm no regression**

```bash
pytest tests/test_ingest_olist.py tests/test_dag_integrity.py -v
```
Expected: all pass — `_ingest_olist_files` remains untouched by this task.

- [ ] **Step 6: Lint and commit**

```bash
ruff check .
git add dags/dbt_pipeline_dag.py tests/test_cloud_deployment.py
git commit -m "Add Olist S3 fallback for ECS Fargate's ephemeral storage"
```

---

### Task 5: Local regression verification

**Files:** none modified — this task only runs checks.

**Interfaces:** none new.

- [ ] **Step 1: Full pytest suite against a live warehouse, in the Linux container**

This is the one step in this plan that needs the Docker network (matches
`tests/test_ingest_olist.py`'s and `tests/conftest.py`'s `warehouse_engine` fixture, unchanged by
this plan). If the dev stack isn't already running:
```bash
docker compose up -d postgres_warehouse
```
Then, from the repo root:
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
Expected: full suite passes, same pass count as before this plan plus the 9 new tests in
`tests/test_cloud_deployment.py`.

- [ ] **Step 2: Confirm the dev Compose stack itself still behaves identically**

```bash
docker compose build
docker compose up -d
```
Wait for `airflow-init` to complete (`docker compose ps` shows it `Exited (0)`), then:
```bash
docker compose exec airflow-scheduler airflow dags list
```
Expected: `dbt_pipeline` (and the other 3 DAGs) still listed, same as before this plan — the
Dockerfile's baked copies are overlaid by the unchanged bind mounts, so nothing about local behavior
should differ. Trigger `dbt_pipeline` manually from the UI (http://localhost:8080) if you want a live
end-to-end confirmation; not required to consider this task done, since Task 1 (Global Constraints)
already established that `OLIST_S3_BUCKET` is unset locally, so the S3 fallback path is inert here by
construction.

- [ ] **Step 3: Leave the stack in whatever state it was in before this task**

If Step 2 started the stack fresh (it wasn't already running before this plan), bring it back down:
```bash
docker compose down
```
If it was already running as part of normal dev workflow, leave it running.

---

## Self-Review

**Spec coverage:** §1 (Task 2) ✅. §2 (Task 3) ✅. §3 (Task 4) ✅. §8's dependency addition only
(Task 1) ✅ — §8's actual remote-logging *configuration* (env vars on ECS task definitions) is Phase
4b, not this plan; noted in this plan's Goal. §13 rows 1-5 (all 4 code tasks' tests) ✅. §13 row 6
(`deploy.yml` structure) is explicitly Phase 4b's, not this plan's — `deploy.yml` doesn't exist until
Phase 4b creates it.

**Placeholder scan:** no TBD/TODO, no "add appropriate error handling," no "similar to Task N"
without repeated code, no untyped function references — clean.

**Type consistency:** `_ensure_olist_data_available(data_dir: Path, files_map: dict[str, str],
bucket: str | None) -> None` — same signature used in Task 4's Step 3 (definition) and Step 1's tests
(all three calls pass `(tmp_path, OLIST_FILES, <str or None>)`, matching). `_warehouse_engine_url()
-> str` — same in Task 2's Step 3 (definition) and Step 1's tests (subprocess prints the return
value). `_download_olist_from_s3(bucket: str, prefix: str, dest: Path) -> None` — called only from
`_ensure_olist_data_available` with keyword args matching this signature exactly.
