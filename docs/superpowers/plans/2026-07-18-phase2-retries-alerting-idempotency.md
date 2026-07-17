# Phase 2: DAG Retries, Alerting & Idempotency Guards Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a shared retry/alerting policy and `max_active_runs=1` concurrency guard to all 4
DAGs, and fix the one real idempotency gap found during design (`rag_index`'s unretried OpenAI
embeddings call).

**Architecture:** A new `dags/_operational_defaults.py` module holds `slack_alert()` (extracted
from `dbt_pipeline_dag.py`) and `operational_default_args()` (a `default_args` dict: 2 retries,
5-minute fixed delay, Slack-on-final-failure). Each of the 4 DAGs imports both and adds them plus
`max_active_runs=1` to its `@dag(...)` decorator. `rag_index_dag.py` additionally gets a small
manual retry-with-backoff wrapper around its OpenAI call.

**Tech Stack:** Airflow 2.9.1 (`default_args`, `max_active_runs`), no new dependencies.

## Global Constraints

- Design spec: `docs/superpowers/specs/2026-07-18-phase2-retries-alerting-idempotency-design.md`
  (read it for full rationale — this plan implements it).
- Python 3.12, Airflow 2.9.1 — same as Phase 0/1. Empirically verified during planning (installing
  `apache-airflow==2.9.1` in a throwaway container and inspecting the source): `BaseOperator`'s own
  defaults are `retries=0`, `retry_delay=0:05:00` (already 5 minutes!), and
  `core.max_active_runs_per_dag` defaults to `16`. This matters for how tests are written below —
  see Task 2's note on why the cascading-verification test checks `.retries`, not `.retry_delay`.
- **Local Windows testing:** `apache-airflow`'s `operators.python` unconditionally does
  `import fcntl` (POSIX-only), so any test file that imports from `dags/*.py` fails to *collect*
  on native Windows (confirmed during Phase 1). Verify every pytest-running step below using a
  disposable Linux container instead, attached to the project's Docker network so it can reach
  `postgres_warehouse`:
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
  (`postgres_warehouse` must already be running: `docker compose up -d postgres_warehouse` from
  the repo root, outside the container.)
- No live Slack webhook is needed for any test in this plan — `slack_alert` tests mock
  `requests.post` directly; they never make a network call.
- `dags/` has no `__init__.py` and is imported as a namespace package via `pythonpath = ["."]`
  (`pyproject.toml`) — same pattern `tests/test_ingest_olist.py` already uses for
  `from dags.dbt_pipeline_dag import ...`. `from dags._operational_defaults import ...` works the
  same way.
- ruff config (`pyproject.toml`): `select = ["E", "F", "I"]`, line-length 100. Run `ruff check .`
  before every commit — do not commit with lint errors outstanding.
- `openai==1.25.0` (pinned in `requirements.txt`) — empirically verified during planning:
  `openai.APIConnectionError(request=httpx.Request("POST", "https://api.openai.com/v1/embeddings"))`
  is a valid construction (message defaults to `"Connection error."`), and `APIConnectionError` is
  a subclass of `APIError`. `httpx` is always present as an `openai` dependency — no new
  requirement to add.

---

### Task 1: Shared operational-defaults module

**Files:**
- Create: `dags/_operational_defaults.py`
- Create: `tests/test_operational_defaults.py`

**Interfaces:**
- Produces: `slack_alert(context: dict) -> None` and `operational_default_args() -> dict` (returns
  `{"retries": 2, "retry_delay": timedelta(minutes=5), "on_failure_callback": slack_alert}`) in
  `dags/_operational_defaults.py` — Task 2 imports both into all 4 DAGs.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_operational_defaults.py`:
```python
from __future__ import annotations

from dags._operational_defaults import operational_default_args, slack_alert


class _FakeTaskInstance:
    dag_id = "test_dag"
    task_id = "test_task"
    log_url = "http://example.com/log"


def test_slack_alert_noops_without_webhook_url(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    calls = []
    monkeypatch.setattr(
        "dags._operational_defaults.requests.post",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    slack_alert({"task_instance": _FakeTaskInstance()})

    assert calls == []


def test_slack_alert_posts_expected_payload(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.example/webhook")
    calls = []
    monkeypatch.setattr(
        "dags._operational_defaults.requests.post",
        lambda url, json: calls.append((url, json)),
    )

    slack_alert({"task_instance": _FakeTaskInstance()})

    assert len(calls) == 1
    url, payload = calls[0]
    assert url == "https://hooks.slack.example/webhook"
    assert "test_dag" in payload["text"]
    assert "test_task" in payload["text"]
    assert "http://example.com/log" in payload["text"]


def test_operational_default_args_shape():
    from datetime import timedelta

    args = operational_default_args()

    assert args["retries"] == 2
    assert args["retry_delay"] == timedelta(minutes=5)
    assert args["on_failure_callback"] is slack_alert
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_operational_defaults.py -v
```
Expected: collection error — `ModuleNotFoundError: No module named 'dags._operational_defaults'`
(the module doesn't exist yet).

- [ ] **Step 3: Create `dags/_operational_defaults.py`**

```python
"""
Shared operational defaults for all DAGs in this project: a uniform
retry policy and Slack failure alerting, kept in one place so the
policy can't drift between DAGs (Phase 2 — see
docs/superpowers/specs/2026-07-18-phase2-retries-alerting-idempotency-design.md).
"""
from __future__ import annotations

import os
from datetime import timedelta

import requests


def slack_alert(context: dict) -> None:
    """Post a failure notification to Slack if a webhook URL is configured.
    Fires once per failed task instance, after Airflow's own retries are
    exhausted (Airflow's on_failure_callback semantics) — not per retry
    attempt, to avoid alert noise on transient blips that self-heal."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        return
    ti = context["task_instance"]
    requests.post(webhook_url, json={
        "text": (
            f":red_circle: *Pipeline failure*\n"
            f"*DAG:* {ti.dag_id}  *Task:* {ti.task_id}\n"
            f"*Log:* {ti.log_url}"
        )
    })


def operational_default_args() -> dict:
    """Shared default_args for all 4 DAGs: retries, retry_delay, and Slack
    failure alerting in one place so the policy can't drift between DAGs."""
    return {
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": slack_alert,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_operational_defaults.py -v
```
Expected: `4 passed`.

- [ ] **Step 5: Lint and commit**

```bash
ruff check .
git add dags/_operational_defaults.py tests/test_operational_defaults.py
git commit -m "Add shared operational-defaults module (retry policy + Slack alerting)"
```

---

### Task 2: Wire shared defaults + max_active_runs into all 4 DAGs

**Files:**
- Modify: `dags/dbt_pipeline_dag.py`
- Modify: `dags/rag_index_dag.py`
- Modify: `dags/mlflow_training_dag.py`
- Modify: `dags/mlflow_training_olist_dag.py`
- Modify: `tests/test_operational_defaults.py`

**Interfaces:**
- Consumes: `operational_default_args`, `slack_alert` from `dags._operational_defaults` (Task 1).

**Why the cascading-verification test checks `.retries`, not `.retry_delay`:** `BaseOperator`'s
own built-in default for `retry_delay` is already `timedelta(minutes=5)` (confirmed in Global
Constraints) — identical to the value this task sets. A per-task `.retry_delay` check couldn't
tell working `default_args` cascading apart from Cosmos silently *not* cascading `default_args`
into its generated tasks (both would show 5 minutes either way). `.retries` has no such ambiguity:
`BaseOperator`'s default is `0`, this task sets `2` — a per-task check that lands on `2` only
happens if cascading genuinely worked.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_operational_defaults.py` (after the existing tests):
```python
from datetime import timedelta
from pathlib import Path

from airflow.models import DagBag

REPO_ROOT = Path(__file__).resolve().parent.parent
DAGS_DIR = REPO_ROOT / "dags"

ALL_DAG_IDS = {"dbt_pipeline", "mlflow_training", "mlflow_training_olist", "rag_index"}


def test_all_dags_have_max_active_runs_one():
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    for dag_id in ALL_DAG_IDS:
        assert dagbag.dags[dag_id].max_active_runs == 1, dag_id


def test_all_dags_have_shared_retry_policy_in_default_args():
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    for dag_id in ALL_DAG_IDS:
        dag = dagbag.dags[dag_id]
        assert dag.default_args.get("retries") == 2, dag_id
        assert dag.default_args.get("retry_delay") == timedelta(minutes=5), dag_id


def test_all_dags_have_shared_slack_alert_on_failure():
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    for dag_id in ALL_DAG_IDS:
        dag = dagbag.dags[dag_id]
        assert dag.default_args.get("on_failure_callback") is slack_alert, dag_id


def test_dbt_pipeline_tasks_inherit_retries_via_cascading():
    """Cosmos generates one Airflow task per dbt model/test inside
    dbt_pipeline's DbtTaskGroup — this confirms default_args cascades to
    those generated tasks too, not just the directly-defined ones
    (ingest_olist, dbt_deps, dbt_seed, dbt_docs_generate)."""
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    dag = dagbag.dags["dbt_pipeline"]
    assert len(dag.tasks) > 4, "expected Cosmos-generated tasks in addition to the 4 direct ones"
    for task in dag.tasks:
        assert task.retries == 2, task.task_id
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_operational_defaults.py -v
```
Expected: the 4 new tests FAIL —
`test_all_dags_have_max_active_runs_one` (current value is Airflow's `max_active_runs_per_dag`
default, `16`, not `1`), `test_all_dags_have_shared_retry_policy_in_default_args` (no DAG's
`default_args` currently has a `"retries"` or `"retry_delay"` key),
`test_all_dags_have_shared_slack_alert_on_failure` (only `dbt_pipeline` has an
`on_failure_callback` at all, and it's a different function object — its own local `slack_alert`,
not the shared one), `test_dbt_pipeline_tasks_inherit_retries_via_cascading` (every task's
`.retries` is `0`, `BaseOperator`'s default).

- [ ] **Step 3: Modify `dags/dbt_pipeline_dag.py`**

Current imports (lines 24-35):
```python
from __future__ import annotations

import os
import shutil
from datetime import datetime
from pathlib import Path

import requests
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode
```

Replace with:
```python
from __future__ import annotations

import os
import shutil
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode

from dags._operational_defaults import operational_default_args
```
(`requests` is removed — it was only used by the local `slack_alert`, which this step also
removes below; keeping the now-unused import would fail `ruff check .`'s `F401` rule.)

Current `slack_alert` function + `@dag(...)` decorator (lines 144-169):
```python
def slack_alert(context: dict) -> None:
    """Post a failure notification to Slack if a webhook URL is configured."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        return
    ti = context["task_instance"]
    requests.post(webhook_url, json={
        "text": (
            f":red_circle: *Pipeline failure*\n"
            f"*DAG:* {ti.dag_id}  *Task:* {ti.task_id}\n"
            f"*Log:* {ti.log_url}"
        )
    })


@dag(
    dag_id="dbt_pipeline",
    description="Seeds raw data then runs and tests all dbt models via Cosmos.",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dbt", "cosmos", "warehouse", "portfolio"],
    doc_md=__doc__,
    default_args={"on_failure_callback": slack_alert},
)
def dbt_pipeline() -> None:
```

Replace with:
```python
@dag(
    dag_id="dbt_pipeline",
    description="Seeds raw data then runs and tests all dbt models via Cosmos.",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "cosmos", "warehouse", "portfolio"],
    doc_md=__doc__,
    default_args=operational_default_args(),
)
def dbt_pipeline() -> None:
```

- [ ] **Step 4: Modify `dags/rag_index_dag.py`**

Current imports (lines 14-21):
```python
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
```

Replace with:
```python
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

from dags._operational_defaults import operational_default_args
```

Current `@dag(...)` decorator (lines 109-117):
```python
@dag(
    dag_id="rag_index",
    description="Embed dbt catalog descriptions into pgvector for semantic search.",
    start_date=datetime(2024, 1, 1),
    schedule=None,         # trigger manually after dbt_pipeline runs
    catchup=False,
    tags=["rag", "pgvector", "catalog"],
)
def rag_index() -> None:
```

Replace with:
```python
@dag(
    dag_id="rag_index",
    description="Embed dbt catalog descriptions into pgvector for semantic search.",
    start_date=datetime(2024, 1, 1),
    schedule=None,         # trigger manually after dbt_pipeline runs
    catchup=False,
    max_active_runs=1,
    tags=["rag", "pgvector", "catalog"],
    default_args=operational_default_args(),
)
def rag_index() -> None:
```

- [ ] **Step 5: Modify `dags/mlflow_training_dag.py`**

Current imports (lines 10-15):
```python
from __future__ import annotations

import os
from datetime import datetime

from airflow.decorators import dag, task
```

Replace with:
```python
from __future__ import annotations

import os
from datetime import datetime

from airflow.decorators import dag, task

from dags._operational_defaults import operational_default_args
```

Current `@dag(...)` decorator (lines 18-26):
```python
@dag(
    dag_id="mlflow_training",
    description="Train a lifetime-value predictor and track with MLflow.",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["mlops", "mlflow", "training"],
)
def mlflow_training() -> None:
```

Replace with:
```python
@dag(
    dag_id="mlflow_training",
    description="Train a lifetime-value predictor and track with MLflow.",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["mlops", "mlflow", "training"],
    default_args=operational_default_args(),
)
def mlflow_training() -> None:
```

- [ ] **Step 6: Modify `dags/mlflow_training_olist_dag.py`**

Current imports (lines 15-20):
```python
from __future__ import annotations

import os
from datetime import datetime

from airflow.decorators import dag, task
```

Replace with:
```python
from __future__ import annotations

import os
from datetime import datetime

from airflow.decorators import dag, task

from dags._operational_defaults import operational_default_args
```

Current `@dag(...)` decorator (lines 23-31):
```python
@dag(
    dag_id="mlflow_training_olist",
    description="Train a lifetime-value predictor on real Olist data and track with MLflow.",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["mlops", "mlflow", "training", "olist"],
)
def mlflow_training_olist() -> None:
```

Replace with:
```python
@dag(
    dag_id="mlflow_training_olist",
    description="Train a lifetime-value predictor on real Olist data and track with MLflow.",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["mlops", "mlflow", "training", "olist"],
    default_args=operational_default_args(),
)
def mlflow_training_olist() -> None:
```

- [ ] **Step 7: Run tests to verify they pass**

```bash
pytest tests/test_operational_defaults.py tests/test_dag_integrity.py -v
```
Expected: all pass — including `test_dbt_pipeline_tasks_inherit_retries_via_cascading`, confirming
`default_args` cascades into Cosmos-generated tasks. If that one test fails while the others pass,
Cosmos's `DbtTaskGroup` is not inheriting `default_args` as expected — stop and investigate
Cosmos's `ExecutionConfig`/`RenderConfig`/`operator_args` before continuing; do not silently work
around it.

- [ ] **Step 8: Lint and commit**

```bash
ruff check .
git add dags/dbt_pipeline_dag.py dags/rag_index_dag.py dags/mlflow_training_dag.py dags/mlflow_training_olist_dag.py tests/test_operational_defaults.py
git commit -m "Wire shared retry/alerting defaults and max_active_runs=1 into all 4 DAGs"
```

---

### Task 3: Retry-with-backoff wrapper for rag_index's OpenAI call

**Files:**
- Modify: `dags/rag_index_dag.py`
- Modify: `tests/test_rag_index.py`

**Interfaces:**
- Produces: `_create_embeddings_with_retry(client, model: str, texts: list[str], max_attempts: int = 3)`
  in `dags/rag_index_dag.py` — used internally by `embed_and_store`; no other task depends on it.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_rag_index.py`. First, update the imports at the top of the file:

Current imports (lines 1-8):
```python
from __future__ import annotations

import sqlalchemy

from dags.rag_index_dag import (
    _ensure_catalog_embeddings_schema,
    _extract_descriptions_from_manifest,
)
```

Replace with:
```python
from __future__ import annotations

import time

import httpx
import pytest
import sqlalchemy
from openai import APIConnectionError

from dags.rag_index_dag import (
    _create_embeddings_with_retry,
    _ensure_catalog_embeddings_schema,
    _extract_descriptions_from_manifest,
)
```

Then add these tests (anywhere after the imports, e.g. right before
`test_catalog_embeddings_upsert_is_idempotent`):
```python
def test_create_embeddings_with_retry_succeeds_first_try():
    class _FakeEmbeddings:
        def create(self, model, input):
            return {"model": model, "input": input}

    class _FakeClient:
        embeddings = _FakeEmbeddings()

    result = _create_embeddings_with_retry(_FakeClient(), "text-embedding-3-small", ["a", "b"])

    assert result == {"model": "text-embedding-3-small", "input": ["a", "b"]}


def test_create_embeddings_with_retry_recovers_after_transient_errors(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    request = httpx.Request("POST", "https://api.openai.com/v1/embeddings")
    attempts = {"count": 0}

    class _FakeEmbeddings:
        def create(self, model, input):
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise APIConnectionError(request=request)
            return "success"

    class _FakeClient:
        embeddings = _FakeEmbeddings()

    result = _create_embeddings_with_retry(_FakeClient(), "text-embedding-3-small", ["a"])

    assert result == "success"
    assert attempts["count"] == 3


def test_create_embeddings_with_retry_gives_up_after_max_attempts(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    request = httpx.Request("POST", "https://api.openai.com/v1/embeddings")

    class _FakeEmbeddings:
        def create(self, model, input):
            raise APIConnectionError(request=request)

    class _FakeClient:
        embeddings = _FakeEmbeddings()

    with pytest.raises(APIConnectionError):
        _create_embeddings_with_retry(_FakeClient(), "text-embedding-3-small", ["a"])
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_rag_index.py -v
```
Expected: collection error — `ImportError: cannot import name '_create_embeddings_with_retry'`
(the function doesn't exist yet).

- [ ] **Step 3: Add `_create_embeddings_with_retry` to `dags/rag_index_dag.py`**

Add this module-level function right after `_ensure_catalog_embeddings_schema` (before the
`@dag` decorator):
```python
def _create_embeddings_with_retry(client, model: str, texts: list[str], max_attempts: int = 3):
    """Calls client.embeddings.create with manual retry+backoff (2s/4s/8s)
    on transient OpenAI API errors — a fast first line of defense before
    Airflow's own 5-minute task-level retry (dags/_operational_defaults.py).
    Re-raises after the final attempt so Airflow's retry still applies if
    this also fails."""
    import time

    from openai import APIError

    delays = [2, 4, 8]
    for attempt in range(max_attempts):
        try:
            return client.embeddings.create(model=model, input=texts)
        except APIError:
            if attempt == max_attempts - 1:
                raise
            time.sleep(delays[attempt])
```

- [ ] **Step 4: Update `embed_and_store`'s call site**

Current code (inside `embed_and_store`, in the `rag_index()` DAG factory):
```python
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=texts,
        )
```

Replace with:
```python
        response = _create_embeddings_with_retry(client, "text-embedding-3-small", texts)
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
pytest tests/test_rag_index.py -v
```
Expected: `6 passed` (the 3 existing tests plus the 3 new ones).

- [ ] **Step 6: Lint and commit**

```bash
ruff check .
git add dags/rag_index_dag.py tests/test_rag_index.py
git commit -m "Add retry-with-backoff wrapper around rag_index's OpenAI embeddings call"
```

---

### Task 4: Full regression run

**Files:** none — verification only.

**Interfaces:** none.

- [ ] **Step 1: Run the full suite together**

Using the Linux-container pattern from Global Constraints (with `postgres_warehouse` already
running):
```bash
ruff check .
pytest -v
```
Expected: all tests pass — `tests/test_dag_integrity.py`, `tests/test_operational_defaults.py`,
`tests/test_ingest_olist.py`, `tests/test_rag_index.py`, `tests/test_dbt_build.py`. Pay particular
attention to `tests/test_dag_integrity.py::test_no_dag_import_errors` — it re-parses all 4 DAGs
from scratch and will catch any import mistake in Task 2's edits (e.g. a circular import between
`dags/_operational_defaults.py` and a DAG file, though none is expected since the shared module
has no dependency on any DAG file).

- [ ] **Step 2: Confirm no leftover references to the old local `slack_alert`**

```bash
grep -rn "def slack_alert" dags/
```
Expected: exactly one match, in `dags/_operational_defaults.py` — confirms
`dags/dbt_pipeline_dag.py`'s local copy was fully removed in Task 2, not left as dead code
alongside the import.
