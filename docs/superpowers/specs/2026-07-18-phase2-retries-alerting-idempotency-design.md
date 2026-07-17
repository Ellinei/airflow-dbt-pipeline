# Phase 2: DAG Retries, Alerting & Idempotency Guards — Design

## Context

Phase 1 (pytest suite, dbt custom tests, CI — see `docs/superpowers/specs/2026-07-17-phase1-testing-ci-design.md`)
gated the project on automated tests. This is Phase 2, next on the project's "production grade"
roadmap (explicitly scoped out of Phase 1 as "DAG retries/alerting/idempotency-guard work").

Reading all 4 DAGs during design turned up the current state:
- **Retries:** zero `retries`/`retry_delay` configured anywhere — every task uses Airflow's bare
  default (no automatic retry on transient failure).
- **Alerting:** only `dbt_pipeline` has a Slack `on_failure_callback`
  (`dags/dbt_pipeline_dag.py:144-156`), and only `dbt_pipeline` — `rag_index`,
  `mlflow_training`, and `mlflow_training_olist` have none. It silently no-ops if
  `SLACK_WEBHOOK_URL` isn't set.
- **Idempotency:** the core data operations already look idempotent by design — Olist ingest
  truncates-then-reloads (`_ingest_olist_files`), dbt materializations are inherently re-runnable,
  and `catalog_embeddings` upserts on conflict (Phase 0 fix). But no DAG sets `max_active_runs`,
  so nothing stops overlapping concurrent runs of the same DAG from racing on the same tables, and
  `rag_index`'s `embed_and_store` sends its entire doc list to OpenAI in one uncapped, unretried
  call — a transient API blip fails the whole task.

**Scope decisions made with the user:**
- All three pillars (retries, alerting, idempotency guards), applied uniformly across all 4 DAGs
  (`dbt_pipeline`, `rag_index`, `mlflow_training`, `mlflow_training_olist`) — not a partial pass.
- Alerting channel: reuse and extend the existing Slack webhook pattern to all 4 DAGs. No new
  channel (no email/PagerDuty) this phase.
- Retry policy: uniform default of `retries=2`, `retry_delay=5 minutes` (fixed, not exponential)
  via a shared `default_args`, with one deliberate per-call exception (see §2).
- Architecture: a shared `dags/_operational_defaults.py` module (not an Airflow `cluster_policy`
  hook, not per-DAG duplication) — see §1 for the trade-off discussion.

**Explicitly out of scope for this phase:** dev/prod environment separation (Phase 3), any
Airflow `cluster_policy`/global-settings mechanism (rejected — see §1), a new alerting channel
beyond Slack, checkpointing/partial-progress recovery for `embed_and_store` (the catalog is
demo-scale; re-embedding everything on retry is wasteful but not incorrect, and not worth the
complexity), retry/idempotency guards for MLflow training runs beyond what's already true by
design (each retry legitimately creates a new MLflow run — not a bug to fix).

---

## 1. Shared operational-defaults module

New `dags/_operational_defaults.py`:

```python
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

Each of the 4 DAGs adds `default_args=operational_default_args()` and `max_active_runs=1` to its
`@dag(...)` decorator. `dbt_pipeline_dag.py` additionally removes its local `slack_alert` in favor
of the shared one (same behavior, same payload shape — a pure move, not a behavior change).

**Considered and rejected:**
- *Airflow `cluster_policy` hook* (inject retries/alerting into every DAG automatically via
  `airflow_local_settings.py`, no per-DAG edits): rejected as more "magic" than 4 DAGs justify —
  retry/alert behavior would be invisible unless you knew to look in a separate settings file, and
  harder to override per-task if a future DAG needs different treatment.
- *Per-DAG duplication* (copy the same `default_args` dict into each file): rejected — the same
  "why does this DAG's retry_delay say 5 min" question would need answering 4 times if the policy
  ever changes.

**Verify empirically during implementation:** `default_args` cascades to every task in a DAG,
including Cosmos's per-model tasks in `dbt_pipeline`'s `DbtTaskGroup` — this is expected Airflow
behavior (Cosmos doesn't override `default_args` unless explicitly configured with its own
`operator_args`), but should be confirmed by inspecting the parsed DAG's task instances, not
assumed.

---

## 2. Idempotency guards & retry safety

`max_active_runs=1` on all 4 DAGs prevents overlapping runs of the same DAG from racing on shared
state — e.g. two manually-triggered `rag_index` runs, or a scheduled `dbt_pipeline` run overlapping
a manual backfill, both truncating/reloading the same Olist tables concurrently.

Blanket task retries are safe to add *because* the underlying operations are already idempotent:
Olist ingest truncates-then-reloads, dbt materializations are inherently re-runnable, and
`catalog_embeddings` upserts on conflict (Phase 0 fix). `mlflow_training` /
`mlflow_training_olist` are a deliberate non-issue: each retry legitimately creates a new MLflow
run (`mlflow.start_run()` always mints a new run_id) — that's correct behavior for ML training,
not something to guard against.

**One real gap, fixed specifically:** `rag_index`'s `embed_and_store` (`dags/rag_index_dag.py:139-182`)
sends the entire doc list to OpenAI in a single `client.embeddings.create(...)` call with no
retry. A transient API error (rate limit, network blip) fails the whole task, and Airflow's
5-minute task-level retry is a slow way to recover from something that would likely succeed in
seconds. Fix: wrap just that call in a small manual retry loop — 3 attempts, 2s/4s/8s backoff, catching
`openai.APIError` (the SDK's base exception, covering rate limits, timeouts, and transient server
errors) and re-raising after the final attempt so Airflow's own task-level retry still applies —
no new dependency (a single wrapped API call doesn't justify adding `tenacity`). This is a fast
first line of defense; Airflow's task-level retry remains the backstop if it's still failing after
that.

---

## 3. Alerting

All 4 DAGs get `on_failure_callback=slack_alert` via the shared `default_args`, firing once per
failed task instance after Airflow's retries are exhausted. `on_retry_callback` is explicitly
**not** wired — alerting on every retry attempt would be noise; the goal is to know when something
needed a human, not that a transient blip happened and self-healed.

---

## 4. Testing

Extends the Phase 1 pytest suite with a new `tests/test_operational_defaults.py`:

| Test | Covers |
|---|---|
| DAG-config assertions (via `DagBag`, same pattern as `tests/test_dag_integrity.py`) | All 4 DAGs have `max_active_runs == 1`, `retries == 2`, `retry_delay == timedelta(minutes=5)`, and `on_failure_callback` is `slack_alert`. |
| `test_slack_alert_noops_without_webhook_url` | Mocked `requests.post`; asserts no call is made when `SLACK_WEBHOOK_URL` is unset. |
| `test_slack_alert_posts_expected_payload` | Mocked `requests.post`; asserts the expected DAG/task/log_url payload when `SLACK_WEBHOOK_URL` is set. |
| OpenAI retry-wrapper tests | Mocked OpenAI client raising transient errors N times then succeeding (asserts retry + recovery), and raising every time (asserts it gives up after 3 attempts rather than retrying forever). |

No live-Postgres dependency for any of these — all pure unit tests with mocked I/O.

---

## Explicitly out of scope

Dev/prod environment separation (Phase 3). Airflow `cluster_policy`/global-settings mechanism
(rejected, see §1). A new alerting channel beyond Slack (e.g. email/PagerDuty). Checkpointing or
partial-progress recovery for `embed_and_store` (demo-scale catalog; re-embedding everything on
retry is wasteful but not incorrect). Retry/idempotency guards for MLflow training runs beyond
what's already true by design.
