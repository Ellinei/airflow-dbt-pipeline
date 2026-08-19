# Phase 4: Cloud Deployment — Design

## Context

Phases 1–3 (pytest + CI, retries/alerting/idempotency, dev/prod environment separation — see the
other specs in this directory) are merged and pushed. Phase 3's spec explicitly named "real cloud
deployment" as a deferred, not-yet-scoped future goal ("cloud-deployable is a *later* goal; this
phase just removes local-only hardcoding, it doesn't build cloud infra"). This is that phase.

**Scope decisions settled with the user via Q&A:**
- Target is **AWS**, and this is a **portfolio/demo deployment** — not always-on production. Optimize
  for low cost and clean teardown, not scale or availability.
- Architecture is **managed services + Terraform**, not a lift-and-shift VM: RDS for Postgres
  (replacing both local Postgres containers), ECS Fargate for the Airflow webserver + scheduler, ECR
  for the image registry, Secrets Manager for credentials.
- **MLflow and OpenMetadata are explicitly out of scope** — only the core Airflow + dbt + warehouse
  path (`dbt_pipeline`) gets cloud infra.
- **Networking is public-subnets-only, no NAT gateway** — security groups restrict inbound traffic
  (webserver port 8080, Postgres port 5432) to the operator's own IP; everything else is
  outbound-only, which public subnets support without a NAT gateway's ~$32–35/month standing charge.
- **CI/CD deploys are manual only** (`workflow_dispatch`) — pushing to `master` never touches AWS or
  incurs cost; the operator deploys on demand when actually running a demo.
- **Webserver exposure is a bare Fargate task public IP + security group** — no Application Load
  Balancer, no TLS/ACM certificate, no domain.
- **Full teardown between demo sessions** (`terraform destroy`, $0 standing cost between demos) —
  the accepted tradeoff is redoing the one-time RDS bootstrap (~10–15 minutes) before each session,
  rather than paying ~$14/month to keep RDS alive and skip re-bootstrapping.
- **CI authenticates to AWS via GitHub OIDC + an IAM role** — no long-lived AWS access keys stored as
  repo secrets.

**Codebase research** (cross-checked directly against the files, not assumed):
- `dags/dbt_pipeline_dag.py:168-169` hardcodes `postgres_warehouse:5432` in `ingest_olist`'s
  SQLAlchemy engine URL — verified by reading the file. Every other DB setting in that same function
  (`WAREHOUSE_DB_USER`/`WAREHOUSE_DB_PASSWORD`/`WAREHOUSE_DB_NAME`) already reads from environment
  variables; only host and port are literals. This is the single call site Phase 3's dev/prod pass
  missed — `dbt_project/profiles.yml` and `rag/query.py` both already parameterize host/port
  correctly. Left as-is, this line makes `ingest_olist` try to reach a hostname that doesn't exist
  anywhere in AWS.
- `Dockerfile` — verified by reading it — only `COPY`s `requirements.txt`. `dags/`, `dbt_project/`,
  and `plugins/` are delivered exclusively via `docker-compose.yml` bind mounts today
  (`./dags:/opt/airflow/dags`, `./dbt_project:/opt/airflow/dbt_project`,
  `./plugins:/opt/airflow/plugins`). ECS Fargate has no bind-mount equivalent — the image itself must
  carry the application code.
- Cosmos's `DbtTaskGroup` uses `LoadMode.DBT_LS` (`dbt_pipeline_dag.py:208-209`), which shells out to
  `dbt ls` at DAG **parse** time and requires `dbt_packages/` to already exist. Locally this is
  guaranteed by the one-shot `airflow-init` Compose service running `dbt deps` once into a bind mount
  the scheduler shares. Two independent Fargate tasks share no filesystem, and ECS has no
  init-container equivalent for this — `dbt deps` has to run at image build time instead, or DAG
  parsing fails silently (the DAG never appears in the UI, no obvious error surfaced).
- `data/olist/*.csv` (~121 MB of real Kaggle data `ingest_olist` loads, see `data/olist/README.md`)
  is `.gitignore`d and never committed. Unlike `dags/`/`dbt_project/`, which are tracked in git and
  therefore visible to a GitHub-hosted CI runner, these files exist nowhere Docker can `COPY` them
  from during an automated build. Committing 121 MB of Kaggle data to work around this would
  contradict the existing `.gitignore` rule and is rejected outright.
- `dbt_project/profiles.yml`'s `prod` target (added in Phase 3) is **already correctly
  parameterized** via `env_var('WAREHOUSE_DB_HOST', 'postgres_warehouse')` /
  `env_var('WAREHOUSE_DB_PORT', '5432')` — verified by reading the file. **No changes needed here**;
  its env vars simply get pointed at the RDS endpoint at deploy time.
- `warehouse-init/01_pgvector.sql` and `02_catalog_embeddings_unique_index.sql` currently run
  automatically only because Postgres's official image executes everything under
  `docker-entrypoint-initdb.d` on first boot of an empty volume (`docker-compose.yml:101`). RDS has no
  equivalent hook. Both scripts' own header comments already document a manual `psql` fallback "for
  existing deployments" — RDS simply makes that fallback the only path, not a new problem to solve.
- `.github/workflows/ci.yml` builds nothing, touches no cloud resources, and is untouched by this
  phase — it remains a separate, always-on test/lint gate.

## Approach

Reuse Phase 3's parameterization wherever possible rather than introducing new configuration
surfaces: no new dbt target (`prod`'s existing env vars just point at RDS), no new `DBT_TARGET`
value. New AWS-specific behavior (S3 fallback for Olist data, remote task logging) is gated behind
env vars that are unset in both local Compose stacks, so local dev/prod behavior is byte-for-byte
unchanged by this phase.

**Rejected alternative:** a lift-and-shift EC2 VM running the existing `docker-compose.yml` as-is.
Simpler and cheaper, but demonstrates only "can run Docker Compose on a rented machine," not the
managed-services/IaC skills (RDS, ECS, Secrets Manager, Terraform) the user wants this phase to show.

**Rejected alternative:** EFS for live DAG/dbt-project sync into Fargate tasks. Adds a new billed
service, per-AZ mount targets, and IAM/security-group surface for a capability (live-editing DAGs
against an environment that's fully torn down between demos) that doesn't match how this stack is
actually used. Baking application code into the image at build time (§2) is simpler and consistent
with the manual-deploy-only decision — any DAG/model change requires a rebuild+push+redeploy, which
is accepted.

**Rejected alternative:** Lambda or other custom automation for the one-time RDS bootstrap (§6).
Phase 3 already established the precedent of treating `governance/setup_roles.sql` as a documented
manual step rather than something to automate; this phase extends the same posture to the two new
bootstrap steps RDS requires, rather than building automation for a portfolio-scale, infrequently-run
process.

---

## 1. Fixing the hardcoded warehouse hostname

`dags/dbt_pipeline_dag.py`'s `ingest_olist` task changes:

```python
f"postgresql+psycopg2://{db_user}:{db_password}@postgres_warehouse:5432/{db_name}"
```
to
```python
db_host = os.getenv("WAREHOUSE_DB_HOST", "postgres_warehouse")
db_port = os.getenv("WAREHOUSE_DB_PORT", "5432")
f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
```

Same fallback defaults as today, so local dev/prod Compose behavior is unchanged. `os` is already
imported in this file. `_ingest_olist_files` itself is untouched.

## 2. Baking `dags/` and `dbt_project/` into the image

`Dockerfile` additions, after the existing pip-install layers:

- `COPY dags/ /opt/airflow/dags/`
- `COPY dbt_project/ /opt/airflow/dbt_project/` — must land as a sibling of `dags/` under
  `/opt/airflow`, because `dbt_pipeline_dag.py:43` computes
  `DBT_PROJECT_PATH = Path(__file__).resolve().parent.parent / "dbt_project"`; getting this path
  wrong means Cosmos silently can't find the project.
- `RUN dbt deps --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project` as a
  build-time step, so `dbt_packages/` exists in the image before the scheduler ever parses DAGs
  (resolves the `LoadMode.DBT_LS` parse-time dependency noted in Context). Verify at implementation
  time whether rendering `profiles.yml`'s `env_var()` calls during `dbt deps` needs throwaway
  build-args for the required vars with no default (`WAREHOUSE_DB_USER`/`PASSWORD`/`NAME`) — it
  should not need an actual database connection, only successful Jinja rendering.
- Do **not** `COPY plugins/` — it is `.gitignore`d and empty on a fresh checkout; `COPY` from a path
  absent in the build context fails the build outright. The base image already ships an empty
  `/opt/airflow/plugins`, which is sufficient since nothing in this project populates it.
- `data/` is deliberately not baked in here — see §3.

Locally, none of this changes existing behavior: `docker-compose.yml`'s bind mounts for `./dags` and
`./dbt_project` still overlay the image's baked copies at container start, so local dev/prod stacks
keep live-editing exactly as today.

## 3. Olist raw data distribution (S3)

One new S3 bucket (also used for logging, §8) gets an `olist-raw/` prefix. The operator uploads the
CSVs once, after `terraform apply`, via `aws s3 sync data/olist/ s3://<bucket>/olist-raw/` —
documented in the README next to the existing Kaggle-download instructions, as the same category of
one-time manual data step.

`ingest_olist` gains a small pre-download step gated on a new `OLIST_S3_BUCKET` env var:

```python
if not all((OLIST_DATA_DIR / f).exists() for f in OLIST_FILES) and os.getenv("OLIST_S3_BUCKET"):
    _download_olist_from_s3(bucket=os.getenv("OLIST_S3_BUCKET"), prefix="olist-raw/", dest=OLIST_DATA_DIR)
```

`OLIST_S3_BUCKET` is unset in both local Compose stacks, so this branch never executes locally —
zero behavior change there. The download target is the ECS task's ephemeral storage (not a
persistent volume): a ~121 MB pull each time a fresh task starts, using `boto3` (already a transitive
dependency once `apache-airflow-providers-amazon` is added for §8, so no new package). `_ingest_olist_files`
itself is untouched, preserving Phase 1's existing unit-test surface for that function.

**Rejected alternative:** `pandas.read_csv("s3://...")` via `s3fs`. Adds a dependency for no benefit
over a plain boto3 download followed by the existing local-file code path.

## 4. Networking

New `terraform/network.tf`:
- One VPC, two public subnets in two AZs (an RDS subnet group requires ≥2 AZs even for a
  single-AZ instance), one Internet Gateway, one public route table associated with both subnets.
  No NAT gateway, no private subnets.
- `ecs_sg`: inbound TCP 8080 from `var.operator_ip/32` only; all outbound allowed (needed to reach
  ECR, RDS, S3, and Secrets Manager).
- `rds_sg`: inbound TCP 5432 from `var.operator_ip/32`, plus a second ingress rule that references
  `ecs_sg` as the source security group (SG-to-SG, not a CIDR). Same-VPC traffic from the ECS tasks
  resolves to the RDS instance's private IP, which never matches the `operator_ip/32` CIDR rule, so
  the ECS tasks need this separate rule to reach RDS at all.
- `var.operator_ip` is a required Terraform variable (no default) — the operator's current public IP,
  re-supplied on each `apply` if it changes between sessions.

## 5. RDS (PostgreSQL, two databases, pgvector)

New `terraform/rds.tf`:
- Engine `postgres`, `engine_version = "15"` (resolves to the latest available 15.x automatically,
  rather than pinning an exact minor that may be deprecated by the time this is implemented). pgvector
  is supported on RDS PostgreSQL from engine version 15.2 onward — **verify current availability**
  against `aws rds describe-db-engine-versions --engine postgres` at implementation time, since RDS
  periodically retires old minors per region.
- Instance class `db.t4g.micro` (cheapest usable size); bump to `db.t4g.small` if the Olist dbt build
  is found to OOM the micro class during implementation.
- `manage_master_user_password = true` — RDS creates and manages the master credential directly in
  Secrets Manager; Terraform's state never contains the plaintext password.
- `publicly_accessible = true`, attached to `rds_sg` (§4) and the two-AZ subnet group.
- `multi_az = false`, `backup_retention_period = 0`, `skip_final_snapshot = true`,
  `deletion_protection = false` — all required, not optional, so `terraform destroy` completes
  cleanly with no orphaned snapshot.
- RDS provisions exactly one initial database at creation (`airflow`, for Airflow's metadata). The
  second database (`warehouse`, for dbt/pgvector) is created manually as part of the one-time
  bootstrap (§6) — `aws_db_instance` has no argument for provisioning a second database.

## 6. One-time bootstrap against a fresh RDS instance

No Lambda, no custom automation — three steps, run once per fresh `terraform apply`, documented as an
ordered README runbook (mirroring how Phase 3 documented the local prod bootstrap):

1. **Airflow metadata migrate + admin user.** A one-off `aws ecs run-task` using the scheduler task
   definition with a `containerOverrides` command
   (`["bash", "-c", "airflow db migrate && airflow users create --username admin ..."]`), launched
   with `assign_public_ip=true` into the same subnet/security group (needed to reach ECR to pull the
   image). Direct port of what the local `airflow-init` Compose service already does, run once instead
   of as a standing service.
2. **Warehouse database + pgvector.** From the operator's IP (already allowed by `rds_sg`):
   `psql -h <rds-endpoint> -U <master-user> -d airflow -c "CREATE DATABASE warehouse;"`, then run
   `warehouse-init/01_pgvector.sql` and `warehouse-init/02_catalog_embeddings_unique_index.sql`
   against the new `warehouse` database — the exact "for existing deployments, run manually" fallback
   both scripts already document, pointed at RDS instead of `localhost:5433`.
3. **Governance roles.** `psql -h <rds-endpoint> -U <master-user> -d warehouse -f governance/setup_roles.sql`
   — the same command already documented in the README for the local prod stack, retargeted at RDS.
   Every dbt model's `grant_select()` post-hook needs the `engineer`/`analyst` roles to exist before
   the first successful `dbt_pipeline` run, exactly as Phase 3 established.

## 7. ECS Fargate services

New `terraform/ecs.tf`:
- One ECS cluster.
- Two task definitions built from the same ECR image, differing only in container command:
  `["webserver"]` and `["scheduler"]` — mirroring how `docker-compose.yml` already splits these two
  roles from one image.
- Two services (`desired_count = 1` each, no autoscaling), both `assign_public_ip = true`, in the
  public subnets from §4, attached to `ecs_sg`.
- Environment variables mirror today's `x-airflow-common-env` (`WAREHOUSE_DB_HOST`/`PORT` now
  pointing at the RDS endpoint, `DBT_TARGET=prod`, `OLIST_S3_BUCKET`, the S3 remote-logging vars from
  §8) plus `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` pointed at RDS's `airflow` database.
- Secrets (DB password, Fernet key, webserver secret key, `OPENAI_API_KEY`, Slack webhook) are
  injected via ECS's native `secrets` (`valueFrom` a Secrets Manager ARN) — see §9 — never passed as
  plain environment variable values and never baked into the image.
- **`AIRFLOW__CORE__FERNET_KEY` and the webserver secret key must resolve to byte-identical values on
  both task definitions** — the webserver decrypts connections the scheduler encrypts (and vice
  versa) — both read from the same Secrets Manager entry, not independently generated per task.

## 8. Logging

Two genuinely different logging concerns, kept separate:

**Airflow's own remote task logging (required, not optional).** With `LocalExecutor` split across two
separate Fargate tasks, the scheduler writes task logs to its own local disk and the webserver — a
different container entirely — has no access to that disk. Without remote logging, every task-log
click in the UI fails. New env vars on both task definitions:
- `AIRFLOW__LOGGING__REMOTE_LOGGING=True`
- `AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=s3://<bucket>/task-logs`
- `AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=aws_default`
- `AIRFLOW_CONN_AWS_DEFAULT=aws://` (an intentionally empty AWS connection URI, so Airflow's S3 hook
  falls through to boto3's default credential chain — which on Fargate resolves to the task's IAM
  role automatically; **no AWS access keys go into Secrets Manager for this**).

New dependency: `apache-airflow-providers-amazon`, added to `requirements.txt`, pinned via the same
`AIRFLOW_CONSTRAINTS_URL` the `Dockerfile` already uses. The `Dockerfile`'s own comments document
60+ minutes of pip resolver backtracking the last time an unpinned package was added to this joint
install — verify this installs cleanly under the existing pinned set empirically before considering
this phase done; budget real time for it.

**Container-level logs (webserver/scheduler process stdout/stderr) are a standard, separate ECS
concern**: the `awslogs` log driver on each task definition, pointed at its own CloudWatch Logs group
(`/ecs/airflow-webserver`, `/ecs/airflow-scheduler`). These groups are created explicitly in
`terraform/logging.tf` with a short `retention_in_days` (e.g. 7) — if left to ECS's
auto-create-on-first-log behavior, they're created outside Terraform's state and `terraform destroy`
orphans them.

**Bucket layout:** one S3 bucket, two prefixes — `task-logs/` (Airflow remote task logging) and
`olist-raw/` (§3) — not two buckets, and not merged with CloudWatch (a different logging subsystem
entirely). `force_destroy = true` on the bucket, plus a short lifecycle rule expiring `task-logs/`
objects after ~14–30 days, keeps storage cost trivial and teardown clean.

## 9. Secrets Manager

New `terraform/secrets.tf`: one JSON secret (`app-secrets`) holding the Fernet key, webserver secret
key, `OPENAI_API_KEY`, and Slack webhook URL, referenced per-key from ECS task definitions via
`valueFrom: <arn>:<json-key>::` — cheaper than five discrete secrets (Secrets Manager bills per
secret). `recovery_window_in_days = 0`, so that a `terraform apply` following a full `terraform
destroy` never collides with a still-pending-deletion secret name (AWS's default recovery window is
7–30 days; omitting this setting would make every fresh `apply` after a teardown fail).

The RDS master password is a second, separate secret managed entirely by RDS itself
(`manage_master_user_password = true`, §5) — Terraform never generates or reads its plaintext value.

## 10. Terraform structure and state

New `terraform/` directory: `network.tf`, `rds.tf`, `ecr.tf` (`force_delete = true`, so
`terraform destroy` succeeds even if images remain), `secrets.tf`, `logging.tf`, `iam.tf` (execution
role using the AWS-managed `AmazonECSTaskExecutionRolePolicy` plus inline Secrets Manager read access;
a separate task role scoped to the S3 bucket's `task-logs/` and `olist-raw/` prefixes), `ecs.tf`,
`oidc.tf` (§11), `outputs.tf` (ECR repo URL, RDS endpoint, cluster/service names, secret ARNs — the
values the operator needs for the README runbook and GitHub Actions), `variables.tf` +
`terraform.tfvars.example`.

**State: local, not S3-backed.** An S3 remote-state backend is more conventional IaC practice, but has
a bootstrapping chicken-and-egg problem (the backend bucket must exist before Terraform can use it as
a backend for the resources it's about to create) and buys nothing for a single-operator,
stand-up/tear-down usage pattern with no team to share state with. `.gitignore` gains
`terraform/*.tfstate*` and `terraform/.terraform/`; `terraform/.terraform.lock.hcl` stays tracked
(standard practice — pins provider versions). Named here as a future enhancement (an S3+DynamoDB
backend, bootstrapped via a small one-time `terraform/bootstrap/` config), not silently dropped.

## 11. CI/CD deploy workflow

New `.github/workflows/deploy.yml`, `workflow_dispatch` only — no `push`/`pull_request` triggers, so
opening a PR or pushing to `master` never touches AWS or costs money.

- Authenticates via GitHub's OIDC provider assuming an IAM role (`terraform/oidc.tf`: an
  `aws_iam_openid_connect_provider` trusting `token.actions.githubusercontent.com`, scoped to this
  repo, plus a role with ECR-push and ECS-update permissions) — no long-lived AWS keys stored as
  GitHub secrets.
- Builds the image from the (now application-code-baked, §2) `Dockerfile` and pushes it to ECR under
  **both** `:latest` and `:${{ github.sha }}` tags.
- Runs `aws ecs update-service --force-new-deployment` for both the webserver and scheduler services,
  which pick up the freshly-pushed `:latest` image.

Task definitions always reference `:latest` — no new task definition revision is registered per
deploy, keeping the mechanic simple. **Tradeoff, accepted:** rollback isn't a one-command operation;
the SHA-tagged image exists in ECR as a safety net, but reverting means manually re-tagging a prior
SHA as `:latest`, pushing that, and forcing another deployment. A git-SHA-tagged
task-definition-revision-per-deploy approach would make rollback a single `aws ecs update-service
--task-definition <family>:<prior-revision>` call, at the cost of one more moving part per deploy —
reasonable to switch to later if this becomes a real operational concern, not needed for a
torn-down-between-sessions demo environment.

`.github/workflows/ci.yml` is untouched.

## 12. Documentation

`README.md` gets a new "Cloud Deployment (AWS)" section: architecture summary (mirroring the diagram
below), prerequisites (AWS CLI, Terraform, an AWS account, the operator's current public IP), the
full runbook in order — `terraform apply` → the three bootstrap steps (§6) → run `deploy.yml` →
look up the webserver task's public IP (`aws ecs describe-tasks` → ENI → public IP; no stable URL
exists without an ALB, which is out of scope) → log in → unpause `dbt_pipeline` (it lands paused per
the existing `DAGS_ARE_PAUSED_AT_CREATION=true` config) → trigger it → confirm task logs render in the
UI — and the `terraform destroy` teardown command. A cost table (§13) is added alongside the existing
Key Design Decisions table.

## 13. Testing

Everything below is static/mocked, consistent with this phase having no automatic CI deploy path and
no live-AWS dependency in the test suite:

| Test | Covers |
|---|---|
| `ingest_olist` host/port parameterization | clean-subprocess import (same pattern as Phase 3's `DBT_TARGET` tests) asserting the engine URL uses `WAREHOUSE_DB_HOST`/`WAREHOUSE_DB_PORT` when set, and falls back to today's literals when unset |
| Olist S3 fallback, `OLIST_S3_BUCKET` unset | local files present → S3 code path never invoked (mocked boto3 client asserts zero calls) — proves local dev/prod behavior is unchanged |
| Olist S3 fallback, `OLIST_S3_BUCKET` set + local files missing | mocked boto3 `download_file` called once per expected key, then `_ingest_olist_files` invoked against the download destination |
| `Dockerfile` structure | text-based assertions that `COPY dags/`, `COPY dbt_project/`, and a `dbt deps` `RUN` line exist, and that no `COPY plugins/` line exists |
| `requirements.txt` | contains `apache-airflow-providers-amazon` |
| `deploy.yml` structure | `on:` is `workflow_dispatch` only — no `push`/`pull_request` triggers (guards against the manual-only decision regressing) |

No live-Postgres or live-AWS dependency for any of these — same tier as the existing DAG-integrity and
environment-separation suites.

---

## Architecture

```
GitHub repo ──(workflow_dispatch, OIDC)──► GitHub Actions ──build/push──► ECR repo
                                                              │
                                                    update-service ×2 (force new deployment)
                                                              ▼
                        AWS Account (one region)
   VPC — 2 public subnets, 2 AZs, IGW + public route table, no NAT

   SG: ecs_sg (in: 8080 from operator IP)     SG: rds_sg (in: 5432 from operator IP + from ecs_sg)

   ECS Fargate "webserver" task ──┐                    ┌── ECS Fargate "scheduler" task
   (same image, cmd=webserver)    │                    │   (same image, cmd=scheduler)
   public IP via ENI              │                    │   public IP via ENI
                                   ▼                    ▼
                     RDS PostgreSQL 15.x (public subnet, publicly_accessible)
                       db: airflow (metadata)   db: warehouse (dbt + pgvector)

                     S3 bucket: /task-logs/ (Airflow remote task logging)
                                /olist-raw/ (9 CSVs, uploaded once by operator)

                     CloudWatch Logs: /ecs/airflow-webserver, /ecs/airflow-scheduler
                     Secrets Manager: RDS-managed master password (auto) +
                                      one app-secrets JSON (Fernet key, webserver
                                      secret key, OPENAI_API_KEY, Slack webhook)
```

## Cost estimate

Approximate `us-east-1` on-demand pricing — re-verify current rates before committing, since AWS
pricing (especially the newer per-hour public IPv4 charge) changes over time.

| Resource | If left running 24/7 | Notes |
|---|---|---|
| Fargate — webserver (0.5 vCPU / 1 GB) | ~$18/mo | |
| Fargate — scheduler (1 vCPU / 2 GB) | ~$36/mo | dbt builds are more CPU/memory-hungry than the webserver; sized up deliberately |
| RDS `db.t4g.micro` + 20 GB gp3 | ~$14/mo | bump to `db.t4g.small` (~+$12/mo) if the Olist build OOMs the micro class |
| Public IPv4 (2 ECS ENIs + 1 RDS) | ~$11/mo | direct cost of public-subnets-no-NAT (§4) — still roughly a third of a NAT gateway's standing charge |
| ECR storage | ~$0.30–0.40/mo | image is ~3–4 GB (see Risks) |
| S3 (task logs + Olist raw data) | ~$0.05/mo | 121 MB Olist data + low log volume |
| Secrets Manager | ~$0.80/mo | 1 RDS-managed secret + 1 app-secrets JSON |
| CloudWatch Logs | ~$0.50/mo | short retention, low volume |
| **Total if left running continuously** | **~$80–85/mo** | not the intended usage pattern |

**Per demo session** (stand-up → bootstrap → trigger a run → review → `terraform destroy`, a few
hours): roughly **$0.30–0.50 total**. Given the full-teardown decision, there is no standing cost
between demo sessions at all.

## Risks / open questions

- **Full teardown re-triggers full bootstrap.** The single-command-teardown goal means the RDS/
  governance/warehouse-init bootstrap (§6) has to be redone before every subsequent demo. Accepted per
  the user's explicit choice; if this friction turns out to be annoying in practice, keeping RDS
  running between demos (~$14/mo, tearing down only ECS/ECR/networking) is a documented alternative,
  not designed this phase.
- **First `terraform apply` has no image to pull yet.** ECS services are created with
  `desired_count = 1` before any image exists in ECR, so both tasks show `CannotPullContainerError`
  until the first `deploy.yml` run completes. Expected and benign — called out in the README runbook
  so it isn't mistaken for a failure.
- **The webserver's public IP changes on every redeploy** (new ENI each time a Fargate task
  restarts) — no stable URL without an ALB (out of scope). The operator looks it up via
  `aws ecs describe-tasks` each time.
- **`DAGS_ARE_PAUSED_AT_CREATION=true`** (existing config) means `dbt_pipeline` lands paused on first
  deploy — easy to mistake for a broken deployment if the runbook doesn't call it out.
- **Image size.** `requirements.txt` still carries `mlflow`, `scikit-learn`, and `openai` even though
  MLflow's own infra is out of scope this phase — the baked image is likely 3–4 GB, meaning the first
  Fargate task start after every deploy spends real time pulling it. Not fixed this phase (slimming
  the image is unrelated scope creep relative to what was asked).
- **`apache-airflow-providers-amazon` version resolution risk** — verify the build empirically before
  considering this phase done (see §8).
- **Which DAGs actually work on this stack, stated explicitly:** `dbt_pipeline` — yes, this phase's
  target. `rag_index` — conditionally (needs `OPENAI_API_KEY` populated and a prior successful
  `dbt_pipeline` run for `manifest.json`); not validated as part of this phase's definition of done.
  `mlflow_training` / `mlflow_training_olist` — expected to fail if triggered (no `mlflow-server`, no
  `MLFLOW_TRACKING_URI`), since MLflow infra is explicitly out of scope.
- No automated "confirm zero orphaned resources" check is built this phase — Goal of a clean
  `terraform destroy` is verified manually via the AWS console/CLI. A small tag-based sweep script is
  a reasonable future enhancement, not built here.

---

## Explicitly out of scope

MLflow and OpenMetadata cloud infra (no infra for `mlflow-server` or the 5 OpenMetadata services).
Application Load Balancer, HTTPS/ACM/custom domain. NAT gateway / private subnets. Autoscaling of any
kind. Multi-AZ RDS. Automatic CI deploy-on-push. EFS for DAG sync (rejected, see `Approach`).
Terraform S3 remote state (deferred, see §10). Slimming the Docker image to drop unused ML
dependencies (flagged in Risks, not fixed this phase). Lambda or other custom RDS-bootstrap
automation (rejected, see `Approach`). Validating `rag_index` or the MLflow DAGs against this stack.
An automated orphaned-resource sweep after `terraform destroy` (manual verification this phase).
