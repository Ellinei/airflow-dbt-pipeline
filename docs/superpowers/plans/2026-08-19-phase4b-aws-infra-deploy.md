# Phase 4b: AWS Infrastructure & Deploy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the Terraform-managed AWS infrastructure (RDS, ECS Fargate, ECR, Secrets Manager,
networking), the manual-only GitHub Actions deploy workflow, and the README runbook that ties it all
together — the cloud-deployment half of Phase 4. Depends on Phase 4a (app code + Dockerfile changes)
being merged first: this plan's ECS task definitions reference the image Phase 4a's Dockerfile
produces, and its `DBT_TARGET=prod`/`WAREHOUSE_DB_HOST`/`OLIST_S3_BUCKET` env vars only work because
Phase 4a wired them up.

**Architecture:** One Terraform root module (`terraform/`) with local state, no remote backend. A
public-subnets-only VPC hosts two ECS Fargate services (webserver, scheduler — same image, different
command) and an RDS PostgreSQL instance, all reachable only from the operator's own IP via security
groups. Secrets Manager holds RDS's auto-managed master password plus two Terraform-created-but-
operator-populated secrets (app credentials, the Airflow metadata DB connection string). A single S3
bucket serves both remote task logging and Olist raw-data distribution. GitHub Actions authenticates
via OIDC (no long-lived AWS keys) and deploys only on manual `workflow_dispatch`, never on push.

**Tech Stack:** Terraform (AWS provider `~> 5.0`), AWS RDS/ECS Fargate/ECR/Secrets Manager/S3/
CloudWatch Logs/IAM, GitHub Actions with OIDC.

**Spec:** `docs/superpowers/specs/2026-08-18-phase4-cloud-deployment-design.md` §4-§13 — read it for
full rationale; this plan implements those sections. §1-§3 (app code) are Phase 4a's, already done.

## Global Constraints

- **`terraform apply` requires explicit user check-in before it runs — do not run it as part of any
  task below.** RDS + two Fargate tasks + three public IPv4s are real, billable AWS resources living
  outside this machine; "process to Phase 4" authorized writing and validating this plan's
  infrastructure code, not spending money on it. Every task's verification stops at `terraform plan`
  (read-only against the AWS API — shows what *would* change, creates nothing). The one exception is
  §6's bootstrap runbook and Task 7's final `terraform apply`/`destroy` instructions, which are
  written as README documentation for the operator to run manually, not executed by this plan.
- **Three facts verified empirically this session against the live AWS account — do not re-derive:**
  - PostgreSQL `15.18` is the latest available `15.x` engine version in `us-east-1` (confirmed via
    `aws rds describe-db-engine-versions --engine postgres`), comfortably above the `15.2` floor
    pgvector requires on RDS. `engine_version = "15"` (auto-resolves to latest 15.x) is safe to use
    as-is — no need to pin an exact patch or re-check availability.
  - `manage_master_user_password = true`'s auto-created Secrets Manager secret is a JSON object with
    (at minimum) `username` and `password` keys — this is Secrets Manager's standard RDS/Aurora
    credential-secret structure, documented for MySQL/MariaDB/PostgreSQL alike. ECS `secrets`
    `valueFrom` entries in this plan reference `<arn>:username::` and `<arn>:password::` on that
    basis.
  - Verified via `aws sts get-caller-identity`: the connected AWS MCP session has credentials for
    account `329005084091`. Confirm this is the intended deployment account before running any
    `terraform plan`/`apply` — do not assume it silently.
- **No plaintext secrets in Terraform state, beyond what AWS itself puts there.** Following the same
  principle §5 states for the RDS master password (`manage_master_user_password = true` specifically
  *so Terraform's state never contains the plaintext password*), this plan extends that principle to
  the two Secrets Manager secrets it creates in Task 2: Terraform creates the secret *shell* only (no
  `aws_secretsmanager_secret_version` resource, no Terraform variable holding the actual credential
  values). The operator populates both via `aws secretsmanager put-secret-value` as part of the
  bootstrap runbook (Task 7) — this is a deliberate deviation from the spec's literal silence on *how*
  `app-secrets` gets its value, decided this session; see Task 2's rationale.
- **A gap the spec's §7 doesn't fully resolve, decided this session:** ECS's native `secrets`
  mechanism injects one secret's raw value (or one JSON key within it) verbatim into one env var — it
  cannot template-combine a literal host/port/dbname with a secret password into one composed URL.
  `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` needs exactly that composition (a full
  `postgresql+psycopg2://user:pass@host:port/db` string), which is why Task 2 creates a second,
  purpose-built secret (`airflow_db_conn`) holding the whole pre-composed string, populated manually
  after RDS exists (Task 7's runbook) — rather than introducing Airflow's `SecretsManagerBackend`
  config surface, which would solve the same problem with strictly more moving parts for a
  single-connection-string need. `WAREHOUSE_DB_USER`/`WAREHOUSE_DB_PASSWORD` don't have this problem —
  dbt's `profiles.yml` already assembles its connection from *separate* env vars (Phase 3), so each
  can be injected as its own single ECS secret value directly from RDS's own managed secret.
- **Terraform version/provider:** target Terraform `>= 1.5`, AWS provider `~> 5.0`. Every task's HCL
  argument names below are the values the spec pins, not verified-correct Terraform syntax — **check
  current argument names against `registry.terraform.io/providers/hashicorp/aws/latest/docs`** for
  each resource before writing it (provider schemas shift between minor versions; writing HCL from
  memory here would read as verified when it isn't). `terraform validate` is the backstop that catches
  a wrong argument name — treat a `validate` failure as "argument name needs checking against current
  docs," not as this plan being wrong about the *value*.
- **Verification chain for every Task 1-6 step that touches `.tf` files:**
  ```bash
  cd terraform
  terraform fmt -check
  terraform init          # first time only
  terraform validate
  terraform plan -out=/dev/null   # read-only; requires AWS credentials but creates nothing
  ```
  Fix `fmt`/`validate` failures before moving to the next task. A `plan` failure due to a missing
  variable (e.g. `operator_ip` not yet in a local `terraform.tfvars`, gitignored) is expected until
  the operator supplies one — note it and move on; it is not a plan/task defect.
- `.gitignore` gains (Task 1): `terraform/*.tfstate*`, `terraform/.terraform/`. `terraform.tfvars`
  itself (the operator's real values, as opposed to the tracked `.example`) should also never be
  committed — add `terraform/terraform.tfvars` to `.gitignore` alongside the state/plugin entries.
  `terraform/.terraform.lock.hcl` stays tracked (pins provider versions, standard practice).

---

### Task 1: Terraform foundation + networking

**Files:**
- Create: `terraform/versions.tf`, `terraform/variables.tf`, `terraform/terraform.tfvars.example`,
  `terraform/network.tf`
- Modify: `.gitignore`

**Interfaces:**
- Produces: `var.aws_region`, `var.operator_ip`, `var.project_name` (consumed by every later task).
  `aws_vpc.main`, two `aws_subnet.public[*]` (or `public_a`/`public_b`) across two AZs,
  `aws_security_group.ecs_sg`, `aws_security_group.rds_sg` — Task 3 (RDS) consumes the subnets +
  `rds_sg`; Task 4 (ECS) consumes the subnets + `ecs_sg`.

- [ ] **Step 1: `terraform/versions.tf`**

Pin the Terraform and AWS provider versions, and configure the provider to read the region from
`var.aws_region`:
- `terraform { required_version = ">= 1.5" }`
- `required_providers { aws = { source = "hashicorp/aws", version = "~> 5.0" } }`
- `provider "aws" { region = var.aws_region }`

- [ ] **Step 2: `terraform/variables.tf`**

| Variable | Type | Default | Notes |
|---|---|---|---|
| `aws_region` | string | `"us-east-1"` | |
| `operator_ip` | string | **none — required** | Bare IP, no `/32` — appended in the SG resource. Spec §4: "no default — the operator's current public IP, re-supplied on each apply if it changes." |
| `project_name` | string | `"airflow-dbt-pipeline"` | Prefix for resource names/tags used throughout every later task. |

- [ ] **Step 3: `terraform/terraform.tfvars.example`**

```hcl
aws_region   = "us-east-1"
operator_ip  = "203.0.113.42"   # your current public IP (curl ifconfig.me) — re-supply if it changes
project_name = "airflow-dbt-pipeline"
```
(A real `terraform.tfvars` — gitignored, see Global Constraints — will also need `github_repo` once
Task 5 adds that variable; not required yet.)

- [ ] **Step 4: `terraform/network.tf`**

Resources, with the spec's pinned decisions (§4) called out:

| Resource | Key arguments |
|---|---|
| `aws_vpc.main` | one VPC, any non-conflicting CIDR (e.g. `10.0.0.0/16`), `enable_dns_support`/`enable_dns_hostnames = true` (needed for RDS endpoint DNS resolution) |
| `aws_subnet.public_a`, `aws_subnet.public_b` | two **public** subnets in **two different AZs** (`data.aws_availability_zones.available` to pick them) — required even for a single-AZ RDS instance, since `aws_db_subnet_group` needs ≥2 AZs. `map_public_ip_on_launch = true`. |
| `aws_internet_gateway.main` | attached to the VPC |
| `aws_route_table.public` + two `aws_route_table_association` | one public route table (`0.0.0.0/0` → the IGW), associated with both subnets |
| `aws_security_group.ecs_sg` | in the VPC. **Ingress:** TCP 8080 from `"${var.operator_ip}/32"` only. **Egress:** allow all (`0.0.0.0/0`, all protocols) — spec §4 explicit: "needed to reach ECR, RDS, S3, and Secrets Manager." |
| `aws_security_group.rds_sg` | in the VPC. **Ingress:** TCP 5432 from `"${var.operator_ip}/32"` only. No egress rule needed — RDS doesn't initiate outbound connections in this design. |

**No NAT gateway, no private subnets** — both subnets are public, spec §4 explicit.

- [ ] **Step 5: `.gitignore`**

Add a new section (after the existing "OpenMetadata" section, end of file):
```
# ── Terraform (Phase 4) ─────────────────────────────────────────────────────
terraform/*.tfstate*
terraform/.terraform/
terraform/terraform.tfvars
```

- [ ] **Step 6: Verify**

```bash
cd terraform
terraform fmt -check
terraform init
terraform validate
```
Expected: `Success! The configuration is valid.` (`terraform plan` needs `operator_ip` supplied —
either via a local, gitignored `terraform.tfvars`, or skip `plan` until Task 6 when every variable
this module needs actually exists; `validate` alone is sufficient here since no resource in this task
references anything outside it yet.)

- [ ] **Step 7: Commit**

```bash
git add terraform/versions.tf terraform/variables.tf terraform/terraform.tfvars.example \
  terraform/network.tf .gitignore
git commit -m "Add Terraform foundation and VPC networking for Phase 4"
```

---

### Task 2: Secrets Manager + Logging

**Files:**
- Create: `terraform/secrets.tf`, `terraform/logging.tf`

**Interfaces:**
- Consumes: `var.project_name` (Task 1).
- Produces: `aws_secretsmanager_secret.app_secrets`, `aws_secretsmanager_secret.airflow_db_conn`
  (Task 4's ECS task definitions reference both by ARN). `aws_s3_bucket.main` (Task 4's task role and
  remote-logging env vars reference it; Task 4a's `_ensure_olist_data_available` already expects a
  bucket with an `olist-raw/` prefix). `aws_cloudwatch_log_group.webserver`,
  `aws_cloudwatch_log_group.scheduler` (Task 4's `awslogs` log driver config references both).

- [ ] **Step 1: `terraform/secrets.tf`**

Two secret *shells* — no `aws_secretsmanager_secret_version` resource for either (see Global
Constraints: values are populated manually post-`apply`, never held in a Terraform variable or
state):

| Resource | Key arguments |
|---|---|
| `aws_secretsmanager_secret.app_secrets` | `name = "${var.project_name}-app-secrets"`, `recovery_window_in_days = 0` (spec §9 pinned — without this, a fresh `apply` after a `destroy` fails on a still-pending-deletion secret name) |
| `aws_secretsmanager_secret.airflow_db_conn` | `name = "${var.project_name}-airflow-db-conn"`, `recovery_window_in_days = 0` (same reasoning) |

`app_secrets`'s eventual JSON value (populated in Task 7's runbook) will have four keys:
`fernet_key`, `webserver_secret_key`, `openai_api_key`, `slack_webhook_url` — Task 4's ECS `secrets`
blocks reference each by `<arn>:<key>::`. `airflow_db_conn`'s value is a single plain string (the
full `postgresql+psycopg2://...` URL), referenced whole (no `::key::` suffix) by Task 4.

- [ ] **Step 2: `terraform/logging.tf`**

| Resource | Key arguments |
|---|---|
| `aws_s3_bucket.main` | `bucket` a globally-unique name (e.g. `"${var.project_name}-${data.aws_caller_identity.current.account_id}"` — add an `aws_caller_identity` data source if not already present from another task), `force_destroy = true` (spec §8 pinned — required for `terraform destroy` to succeed with objects still in the bucket) |
| `aws_s3_bucket_lifecycle_configuration` on `aws_s3_bucket.main` | one rule scoped to `filter { prefix = "task-logs/" }`, `expiration { days = 30 }` (spec §8: "~14-30 days"; 30 chosen for concreteness — no rule for `olist-raw/`, which the operator manages manually per §3) |
| `aws_cloudwatch_log_group.webserver` | `name = "/ecs/airflow-webserver"`, `retention_in_days = 7` (spec §8 pinned: "created explicitly ... if left to ECS's auto-create-on-first-log behavior, they're created outside Terraform's state and `terraform destroy` orphans them") |
| `aws_cloudwatch_log_group.scheduler` | `name = "/ecs/airflow-scheduler"`, `retention_in_days = 7` |

Bucket layout is two prefixes in this one bucket — `task-logs/` and `olist-raw/` — not two buckets,
per spec §8 explicit.

- [ ] **Step 3: Verify**

```bash
cd terraform
terraform fmt -check
terraform validate
```
Expected: `Success!`.

- [ ] **Step 4: Commit**

```bash
git add terraform/secrets.tf terraform/logging.tf
git commit -m "Add Secrets Manager shells and S3/CloudWatch logging for Phase 4"
```

---

### Task 3: RDS

**Files:**
- Create: `terraform/rds.tf`

**Interfaces:**
- Consumes: `aws_subnet.public_a`/`public_b`, `aws_security_group.rds_sg` (Task 1); `var.db_instance_class`
  (new variable, added in this task).
- Produces: `aws_db_instance.main` — Task 4 consumes its endpoint (`aws_db_instance.main.endpoint` /
  `.address` + `.port`) for `WAREHOUSE_DB_HOST`/`PORT` and for the `airflow_db_conn` secret's
  composed URL (populated manually, Task 7), and its
  `aws_db_instance.main.master_user_secret[0].secret_arn` for Task 4's ECS `secrets` (`WAREHOUSE_DB_USER`/
  `WAREHOUSE_DB_PASSWORD`) and Task 4's IAM execution-role policy.

- [ ] **Step 1: Add `db_instance_class` and `db_master_username` to `terraform/variables.tf`**

| Variable | Type | Default | Notes |
|---|---|---|---|
| `db_instance_class` | string | `"db.t4g.micro"` | Spec §5: "bump to `db.t4g.small` if the Olist dbt build is found to OOM the micro class during implementation." |
| `db_master_username` | string | `"dbadmin"` | Not `"admin"` — RDS PostgreSQL rejects several reserved master usernames (`admin`, `rdsadmin`, `public` among them); `dbadmin` avoids the collision. |

- [ ] **Step 2: `terraform/rds.tf`**

| Resource | Key arguments |
|---|---|
| `aws_db_subnet_group.main` | `subnet_ids = [aws_subnet.public_a.id, aws_subnet.public_b.id]` |
| `aws_db_instance.main` | `engine = "postgres"`, `engine_version = "15"` (verified this session: resolves to `15.18` in `us-east-1`, above the `15.2` pgvector floor — spec §5), `instance_class = var.db_instance_class`, `allocated_storage = 20`, `storage_type = "gp3"`, `db_name = "airflow"` (the *only* database RDS provisions at creation — `warehouse` is created manually in Task 7's bootstrap runbook, spec §5 explicit: `aws_db_instance` has no argument for a second database), `username = var.db_master_username`, `manage_master_user_password = true` (spec §5 pinned — **no `password` argument**, mutually exclusive with this), `publicly_accessible = true`, `vpc_security_group_ids = [aws_security_group.rds_sg.id]`, `db_subnet_group_name = aws_db_subnet_group.main.name`, `multi_az = false`, `backup_retention_period = 0`, `skip_final_snapshot = true`, `deletion_protection = false` (spec §5: all four "required, not optional, so `terraform destroy` completes cleanly with no orphaned snapshot") |

- [ ] **Step 3: Verify**

```bash
cd terraform
terraform fmt -check
terraform validate
```
Expected: `Success!`. Also spot-check the RDS engine-version fact still holds (cheap, read-only, no
plan needed):
```bash
aws rds describe-db-engine-versions --engine postgres --region us-east-1 \
  --query "DBEngineVersions[?starts_with(EngineVersion, '15.')].EngineVersion"
```
Expected: `15.18` (or a newer 15.x — AWS adds patch versions over time; anything ≥ `15.2` is fine for
pgvector).

- [ ] **Step 4: Commit**

```bash
git add terraform/variables.tf terraform/rds.tf
git commit -m "Add RDS PostgreSQL instance for Phase 4"
```

---

### Task 4: IAM + ECR + ECS Fargate services

**Files:**
- Create: `terraform/ecr.tf`, `terraform/iam.tf`, `terraform/ecs.tf`

**Interfaces:**
- Consumes: `aws_security_group.ecs_sg`, `aws_subnet.public_a`/`public_b` (Task 1);
  `aws_secretsmanager_secret.app_secrets`/`airflow_db_conn`, `aws_s3_bucket.main`,
  `aws_cloudwatch_log_group.webserver`/`scheduler` (Task 2); `aws_db_instance.main` + its
  `master_user_secret[0].secret_arn` (Task 3).
- Produces: `aws_ecr_repository.main` (Task 5's `deploy.yml` pushes to it; Task 6's outputs expose its
  URL). `aws_ecs_cluster.main`, `aws_ecs_service.webserver`/`scheduler` (Task 5's `deploy.yml` runs
  `aws ecs update-service --force-new-deployment` against both; Task 7's bootstrap runbook `run-task`s
  against the scheduler's task definition family).

- [ ] **Step 1: `terraform/ecr.tf`**

| Resource | Key arguments |
|---|---|
| `aws_ecr_repository.main` | `name = var.project_name`, `image_tag_mutability = "MUTABLE"` (spec §11: both `:latest` and `:${{ github.sha }}` tags get pushed — mutable tags required for `:latest` to be re-pushable), `force_delete = true` (spec §10 pinned — "so `terraform destroy` succeeds even if images remain") |

- [ ] **Step 2: `terraform/iam.tf`**

Two roles:

| Resource | Purpose | Key arguments |
|---|---|---|
| `aws_iam_role.ecs_execution` | ECS agent: pull image from ECR, write to CloudWatch, fetch secrets | `assume_role_policy` trusts `ecs-tasks.amazonaws.com`. Attach AWS-managed `AmazonECSTaskExecutionRolePolicy` via `aws_iam_role_policy_attachment`. Add one inline `aws_iam_role_policy` granting `secretsmanager:GetSecretValue` on exactly three ARNs: `aws_db_instance.main.master_user_secret[0].secret_arn`, `aws_secretsmanager_secret.app_secrets.arn`, `aws_secretsmanager_secret.airflow_db_conn.arn` — spec §10: "execution role using the AWS-managed `AmazonECSTaskExecutionRolePolicy` plus inline Secrets Manager read access." |
| `aws_iam_role.ecs_task` | The running Airflow process: S3 for remote logging + Olist download, via boto3's default credential chain (`AIRFLOW_CONN_AWS_DEFAULT=aws://`, no access keys — spec §8) | `assume_role_policy` trusts `ecs-tasks.amazonaws.com`. Inline policy: `s3:GetObject`/`s3:PutObject` on `"${aws_s3_bucket.main.arn}/task-logs/*"`, `s3:GetObject` on `"${aws_s3_bucket.main.arn}/olist-raw/*"`, `s3:ListBucket` on `aws_s3_bucket.main.arn` — spec §10: "a separate task role scoped to the S3 bucket's `task-logs/` and `olist-raw/` prefixes." |

- [ ] **Step 3: `terraform/ecs.tf`**

| Resource | Key arguments |
|---|---|
| `aws_ecs_cluster.main` | `name = var.project_name` |
| `aws_ecs_task_definition.webserver` | `family = "${var.project_name}-webserver"`, `requires_compatibilities = ["FARGATE"]`, `network_mode = "awsvpc"`, `cpu = "512"`, `memory = "1024"` (spec's cost table §13/cost estimate: "0.5 vCPU / 1 GB"), `execution_role_arn = aws_iam_role.ecs_execution.arn`, `task_role_arn = aws_iam_role.ecs_task.arn`, `container_definitions` — see table below |
| `aws_ecs_task_definition.scheduler` | Same shape, `family = "${var.project_name}-scheduler"`, `cpu = "1024"`, `memory = "2048"` (cost table: "1 vCPU / 2 GB — dbt builds are more CPU/memory-hungry than the webserver") |
| `aws_ecs_service.webserver` | `desired_count = 1`, `launch_type = "FARGATE"`, `network_configuration { subnets = [both public subnets], security_groups = [ecs_sg], assign_public_ip = true }` |
| `aws_ecs_service.scheduler` | Same shape |

Both task definitions' `container_definitions` (JSON via `jsonencode`) share this shape — one
container named `webserver`/`scheduler`, `image = "${aws_ecr_repository.main.repository_url}:latest"`,
`command = ["webserver"]` / `["scheduler"]` (mirrors how `docker-compose.yml` already splits these two
roles off one image), `essential = true`, `logConfiguration` using the `awslogs` driver pointed at
this container's own log group from Task 2 (`awslogs-group`, `awslogs-region = var.aws_region`,
`awslogs-stream-prefix = "ecs"`). Only the webserver container needs `portMappings` (container port
`8080`).

**Plain `environment` entries (both containers, mirroring `docker-compose.yml`'s
`x-airflow-common-env`, spec §7):**

| Name | Value |
|---|---|
| `PYTHONPATH` | `/opt/airflow` |
| `AIRFLOW__CORE__EXECUTOR` | `LocalExecutor` |
| `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` | `true` |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `false` |
| `AIRFLOW__API__AUTH_BACKENDS` | `airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session` |
| `WAREHOUSE_DB_HOST` | `aws_db_instance.main.address` |
| `WAREHOUSE_DB_PORT` | `tostring(aws_db_instance.main.port)` |
| `WAREHOUSE_DB_NAME` | `warehouse` |
| `DBT_TARGET` | `prod` |
| `OLIST_S3_BUCKET` | `aws_s3_bucket.main.bucket` |
| `OPENLINEAGE_DISABLED` | `true` |
| `AIRFLOW__LOGGING__REMOTE_LOGGING` | `True` |
| `AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER` | `s3://${aws_s3_bucket.main.bucket}/task-logs` |
| `AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID` | `aws_default` |
| `AIRFLOW_CONN_AWS_DEFAULT` | `aws://` (spec §8: intentionally empty URI, falls through to the task role's credentials) |

**`secrets` entries (both containers — `valueFrom`, ECS native secret injection, spec §9/§7):**

| Name | `valueFrom` |
|---|---|
| `WAREHOUSE_DB_USER` | `"${aws_db_instance.main.master_user_secret[0].secret_arn}:username::"` |
| `WAREHOUSE_DB_PASSWORD` | `"${aws_db_instance.main.master_user_secret[0].secret_arn}:password::"` |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `aws_secretsmanager_secret.airflow_db_conn.arn` (whole value — see Global Constraints) |
| `AIRFLOW__CORE__FERNET_KEY` | `"${aws_secretsmanager_secret.app_secrets.arn}:fernet_key::"` |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | `"${aws_secretsmanager_secret.app_secrets.arn}:webserver_secret_key::"` |
| `OPENAI_API_KEY` | `"${aws_secretsmanager_secret.app_secrets.arn}:openai_api_key::"` |
| `SLACK_WEBHOOK_URL` | `"${aws_secretsmanager_secret.app_secrets.arn}:slack_webhook_url::"` |

**`AIRFLOW__CORE__FERNET_KEY` and `AIRFLOW__WEBSERVER__SECRET_KEY` must be byte-identical on both task
definitions** — spec §7 explicit ("the webserver decrypts connections the scheduler encrypts, and
vice versa"). Both read from the *same* `app_secrets` ARN/keys above on both task definitions, so
this holds by construction — do not generate or reference them independently per task definition.

- [ ] **Step 4: Verify**

```bash
cd terraform
terraform fmt -check
terraform validate
```
Expected: `Success!`. This is also the first point where a `terraform plan` (with a real
`terraform.tfvars` supplying `operator_ip`) becomes meaningful end-to-end — run it and read through
the full resource list it proposes creating; still read-only, still no `apply`.

- [ ] **Step 5: Commit**

```bash
git add terraform/ecr.tf terraform/iam.tf terraform/ecs.tf
git commit -m "Add ECR, IAM roles, and ECS Fargate services for Phase 4"
```

---

### Task 5: GitHub OIDC + CI/CD deploy workflow

**Files:**
- Create: `terraform/oidc.tf`, `.github/workflows/deploy.yml`, `tests/test_deploy_workflow.py`

**Interfaces:**
- Consumes: `aws_ecr_repository.main`, `aws_ecs_cluster.main`, `aws_ecs_service.webserver`/`scheduler`
  (Task 4); new `var.github_repo`.
- Produces: `aws_iam_role.github_deploy` — Task 6's outputs expose its ARN for the operator to
  configure `deploy.yml`'s OIDC role assumption (or `deploy.yml` reads it from a repo variable set
  once from that output — either is fine, document whichever is chosen in Task 7).

- [ ] **Step 1: Add `github_repo` to `terraform/variables.tf`**

| Variable | Type | Default | Notes |
|---|---|---|---|
| `github_repo` | string | **none — required** | `"owner/repo"` this OIDC role trusts, e.g. `"your-username/airflow-dbt-pipeline"`. |

- [ ] **Step 2: `terraform/oidc.tf`**

| Resource | Key arguments |
|---|---|
| `aws_iam_openid_connect_provider.github` | `url = "https://token.actions.githubusercontent.com"`, `client_id_list = ["sts.amazonaws.com"]`. **Verify current guidance on the `thumbprint_list` argument before writing this** — AWS has changed how strictly it validates GitHub's OIDC thumbprint over time; check `registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_openid_connect_provider` and current AWS documentation for whether a `tls_certificate` data source is still the recommended way to obtain it, or whether a fixed/well-known value is now accepted. Do not guess a thumbprint value. |
| `aws_iam_role.github_deploy` | `assume_role_policy`: trusts `aws_iam_openid_connect_provider.github.arn` via `sts:AssumeRoleWithWebIdentity`, condition `StringEquals` on `token.actions.githubusercontent.com:aud = "sts.amazonaws.com"` and `StringLike` on `token.actions.githubusercontent.com:sub = "repo:${var.github_repo}:*"` — spec §11: "no long-lived AWS keys stored as GitHub secrets" |
| `aws_iam_role_policy.github_deploy` | Inline policy: `ecr:GetAuthorizationToken` (Resource `*` — required, this action doesn't support resource-level restriction); `ecr:BatchCheckLayerAvailability`/`GetDownloadUrlForLayer`/`BatchGetImage`/`PutImage`/`InitiateLayerUpload`/`UploadLayerPart`/`CompleteLayerUpload` scoped to `aws_ecr_repository.main.arn`; `ecs:UpdateService`/`DescribeServices` scoped to `[aws_ecs_service.webserver.id, aws_ecs_service.scheduler.id]` — spec §11: "ECR-push and ECS-update permissions" |

- [ ] **Step 3: Write the failing test**

Create `tests/test_deploy_workflow.py`:
```python
from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_deploy_workflow_is_manual_dispatch_only():
    workflow = yaml.safe_load((REPO_ROOT / ".github" / "workflows" / "deploy.yml").read_text())
    # YAML parses the bare `on:` key as the boolean True, not the string "on"
    triggers = workflow[True] if True in workflow else workflow["on"]
    assert set(triggers) == {"workflow_dispatch"}


def test_ci_workflow_is_untouched_by_deploy_triggers():
    ci = yaml.safe_load((REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text())
    triggers = ci[True] if True in ci else ci["on"]
    assert set(triggers) == {"push", "pull_request"}
```

This test only does `yaml.safe_load` on static files, no `dags.*` import — runs natively on Windows:
```bash
pytest tests/test_deploy_workflow.py -v
```
Expected: `test_deploy_workflow_is_manual_dispatch_only` FAILS (`.github/workflows/deploy.yml`
doesn't exist yet); `test_ci_workflow_is_untouched_by_deploy_triggers` already passes (confirms
`ci.yml`'s triggers are what this plan assumes, and guards against `deploy.yml`'s creation
accidentally touching `ci.yml`).

- [ ] **Step 4: `.github/workflows/deploy.yml`**

```yaml
name: Deploy

on:
  workflow_dispatch:

permissions:
  id-token: write
  contents: read

env:
  AWS_REGION: us-east-1
  ECR_REPOSITORY: airflow-dbt-pipeline
  ECS_CLUSTER: airflow-dbt-pipeline

jobs:
  deploy:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Configure AWS credentials via OIDC
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_DEPLOY_ROLE_ARN }}
          aws-region: ${{ env.AWS_REGION }}

      - name: Login to Amazon ECR
        id: ecr-login
        uses: aws-actions/amazon-ecr-login@v2

      - name: Build and push image
        env:
          ECR_REGISTRY: ${{ steps.ecr-login.outputs.registry }}
        run: |
          docker build -t "$ECR_REGISTRY/$ECR_REPOSITORY:latest" \
                        -t "$ECR_REGISTRY/$ECR_REPOSITORY:${{ github.sha }}" .
          docker push "$ECR_REGISTRY/$ECR_REPOSITORY:latest"
          docker push "$ECR_REGISTRY/$ECR_REPOSITORY:${{ github.sha }}"

      - name: Force new deployment (webserver)
        run: |
          aws ecs update-service --cluster "$ECS_CLUSTER" \
            --service "${ECS_CLUSTER}-webserver" --force-new-deployment

      - name: Force new deployment (scheduler)
        run: |
          aws ecs update-service --cluster "$ECS_CLUSTER" \
            --service "${ECS_CLUSTER}-scheduler" --force-new-deployment
```

`AWS_DEPLOY_ROLE_ARN` is a GitHub repo secret the operator sets once, from
`terraform output github_deploy_role_arn` (Task 6) — document this in Task 7's runbook. Task
definitions always reference `:latest`, so no new revision is registered per deploy — spec §11's
accepted tradeoff (rollback means manually re-tagging a prior SHA as `:latest` and forcing another
deployment; the SHA tag exists in ECR as the safety net for that).

**Three literals in this workflow must reconcile with Task 4's actual Terraform resource names —
verify all three, not just the one called out below:**
- `ECR_REPOSITORY: airflow-dbt-pipeline` must equal Task 4's `aws_ecr_repository.main`'s `name`
  argument (`var.project_name`, default `"airflow-dbt-pipeline"` from Task 1).
- `ECS_CLUSTER: airflow-dbt-pipeline` must equal Task 4's `aws_ecs_cluster.main`'s `name` argument
  (also `var.project_name`).
- `${ECS_CLUSTER}-webserver` / `${ECS_CLUSTER}-scheduler` must equal Task 4's
  `aws_ecs_service.webserver`/`scheduler`'s actual `name` arguments literally.

All three derive from `var.project_name` if Task 4 is implemented exactly as its resource table
specifies — so these literals are correct **as long as `project_name` is never changed from its
`"airflow-dbt-pipeline"` default**. If a future session changes `project_name`, this workflow's `env:`
block must be updated to match (it does not read the value from Terraform automatically — GitHub
Actions has no Terraform state access at workflow-run time). Treat this `env:` block as the source of
truth for these three names once written, and keep it in sync with `terraform/variables.tf`'s default
by hand.

`.github/workflows/ci.yml` is untouched by this task — spec §11 explicit.

- [ ] **Step 5: Run test to verify it passes**

```bash
pytest tests/test_deploy_workflow.py -v
```
Expected: `2 passed`.

- [ ] **Step 6: Verify Terraform**

```bash
cd terraform
terraform fmt -check
terraform validate
```
Expected: `Success!`.

- [ ] **Step 7: Lint and commit**

```bash
ruff check .
git add terraform/variables.tf terraform/oidc.tf .github/workflows/deploy.yml tests/test_deploy_workflow.py
git commit -m "Add GitHub OIDC role and manual-dispatch deploy workflow"
```

---

### Task 6: Terraform outputs

**Files:**
- Create: `terraform/outputs.tf`

**Interfaces:**
- Consumes: every resource created in Tasks 1-5.
- Produces: the values Task 7's README runbook and the operator's one-time GitHub secret setup need.

- [ ] **Step 1: `terraform/outputs.tf`**

| Output | Value | Used for |
|---|---|---|
| `ecr_repository_url` | `aws_ecr_repository.main.repository_url` | manual `docker push` before the first `deploy.yml` run ever succeeds (spec's Risks: first `apply` has no image to pull yet) |
| `rds_endpoint` | `aws_db_instance.main.endpoint` | Task 7 runbook step 2 (`psql -h ...`) |
| `rds_master_secret_arn` | `aws_db_instance.main.master_user_secret[0].secret_arn` | Task 7 runbook: retrieving the master password to compose the `airflow_db_conn` secret value, and for direct `psql` access |
| `app_secrets_arn` | `aws_secretsmanager_secret.app_secrets.arn` | Task 7 runbook: `put-secret-value` target |
| `airflow_db_conn_secret_arn` | `aws_secretsmanager_secret.airflow_db_conn.arn` | Task 7 runbook: `put-secret-value` target |
| `s3_bucket_name` | `aws_s3_bucket.main.bucket` | Task 7 runbook: `aws s3 sync data/olist/ ...` |
| `ecs_cluster_name` | `aws_ecs_cluster.main.name` | Task 7 runbook: bootstrap `run-task`, `describe-tasks` for the webserver's public IP |
| `github_deploy_role_arn` | `aws_iam_role.github_deploy.arn` | Task 7 runbook: the operator sets this as the `AWS_DEPLOY_ROLE_ARN` GitHub repo secret once |

- [ ] **Step 2: Verify**

```bash
cd terraform
terraform fmt -check
terraform validate
terraform plan -out=/dev/null
```
Expected: `Success!` and a full, coherent plan (still requires a real `terraform.tfvars` with
`operator_ip` + `github_repo`; still creates nothing).

- [ ] **Step 3: Commit**

```bash
git add terraform/outputs.tf
git commit -m "Add Terraform outputs for Phase 4 bootstrap runbook"
```

---

### Task 7: Documentation + final verification

**Files:**
- Modify: `README.md`

**Interfaces:** none new — this task only documents Tasks 1-6's outputs and the manual bootstrap
sequence (spec §6).

- [ ] **Step 1: Add a "Cloud Deployment (AWS)" section to `README.md`**

Insert it as a new top-level `## Cloud Deployment (AWS)` section, placed directly before the existing
`## Key Design Decisions` section (spec §12: "A cost table is added alongside the existing Key Design
Decisions table"). Contents, in order:

1. **Architecture summary** — one short paragraph plus the diagram from the spec's `## Architecture`
   section (`docs/superpowers/specs/2026-08-18-phase4-cloud-deployment-design.md`), reproduced or
   linked.
2. **Prerequisites** — AWS CLI configured, Terraform `>= 1.5`, an AWS account, the operator's current
   public IP (`curl ifconfig.me`).
3. **First-time setup:**
   ```bash
   cd terraform
   cp terraform.tfvars.example terraform.tfvars   # fill in operator_ip and github_repo
   terraform init
   terraform apply
   ```
4. **Populate the two operator-managed secrets** (Global Constraints/Task 2 — must happen after
   `apply`, since both depend on the RDS master secret existing):
   ```bash
   # Fernet key — same command .env.example already documents for local dev:
   python -c "import base64, secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())"

   RDS_MASTER_SECRET=$(terraform output -raw rds_master_secret_arn)
   RDS_USER=$(aws secretsmanager get-secret-value --secret-id "$RDS_MASTER_SECRET" \
     --query SecretString --output text | python -c "import json,sys; print(json.load(sys.stdin)['username'])")
   RDS_PASS=$(aws secretsmanager get-secret-value --secret-id "$RDS_MASTER_SECRET" \
     --query SecretString --output text | python -c "import json,sys; print(json.load(sys.stdin)['password'])")
   RDS_ENDPOINT=$(terraform output -raw rds_endpoint)

   aws secretsmanager put-secret-value \
     --secret-id "$(terraform output -raw app_secrets_arn)" \
     --secret-string "{\"fernet_key\":\"<paste from above>\",\"webserver_secret_key\":\"<generate the same way>\",\"openai_api_key\":\"<your key, or empty string>\",\"slack_webhook_url\":\"<your webhook, or empty string>\"}"

   aws secretsmanager put-secret-value \
     --secret-id "$(terraform output -raw airflow_db_conn_secret_arn)" \
     --secret-string "postgresql+psycopg2://${RDS_USER}:${RDS_PASS}@${RDS_ENDPOINT}/airflow"
   ```
5. **One-time RDS bootstrap** (spec §6, three ordered steps):
   1. Airflow metadata migrate + admin user, via a one-off `aws ecs run-task` against the scheduler
      task definition with a `containerOverrides` command
      (`["bash", "-c", "airflow db migrate && airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password <choose one>"]`),
      `--launch-type FARGATE`, into the same subnets/`ecs_sg`, `assignPublicIp=ENABLED` (needed to
      reach ECR).
   2. Warehouse database + pgvector, from the operator's IP:
      ```bash
      psql -h "$RDS_ENDPOINT" -U "$RDS_USER" -d airflow -c "CREATE DATABASE warehouse;"
      psql -h "$RDS_ENDPOINT" -U "$RDS_USER" -d warehouse -f warehouse-init/01_pgvector.sql
      psql -h "$RDS_ENDPOINT" -U "$RDS_USER" -d warehouse -f warehouse-init/02_catalog_embeddings_unique_index.sql
      ```
   3. Governance roles:
      ```bash
      psql -h "$RDS_ENDPOINT" -U "$RDS_USER" -d warehouse \
        -v engineer_password="$GOVERNANCE_ENGINEER_PASSWORD" \
        -v analyst_password="$GOVERNANCE_ANALYST_PASSWORD" \
        -f governance/setup_roles.sql
      ```
6. **One-time GitHub setup:** set the `AWS_DEPLOY_ROLE_ARN` repo secret to
   `terraform output -raw github_deploy_role_arn`.
7. **Upload the Olist data once:**
   `aws s3 sync data/olist/ s3://$(terraform output -raw s3_bucket_name)/olist-raw/`.
8. **Deploy:** run the `deploy.yml` workflow manually (GitHub Actions UI or
   `gh workflow run deploy.yml`).
9. **Find the webserver's public IP** (no stable URL without an ALB, out of scope — spec Risks):
   ```bash
   TASK_ARN=$(aws ecs list-tasks --cluster "$(terraform output -raw ecs_cluster_name)" \
     --service-name "$(terraform output -raw ecs_cluster_name)-webserver" --query 'taskArns[0]' --output text)
   ENI_ID=$(aws ecs describe-tasks --cluster "$(terraform output -raw ecs_cluster_name)" --tasks "$TASK_ARN" \
     --query 'tasks[0].attachments[0].details[?name==`networkInterfaceId`].value' --output text)
   aws ec2 describe-network-interfaces --network-interface-ids "$ENI_ID" \
     --query 'NetworkInterfaces[0].Association.PublicIp' --output text
   ```
10. **Log in, unpause `dbt_pipeline`, trigger it, confirm task logs render in the UI** — it lands
    paused per `DAGS_ARE_PAUSED_AT_CREATION=true`, easy to mistake for a broken deploy if not called
    out (spec Risks).
11. **Teardown:** `terraform destroy` — full teardown between demo sessions, $0 standing cost; the
    tradeoff is redoing steps 4-7 (~10-15 min) before the next session (spec's accepted tradeoff, §
    "Risks").

- [ ] **Step 2: Add the cost table**

Reproduce the spec's `## Cost estimate` table verbatim (re-verify the per-hour public IPv4 rate is
still current before publishing — it was confirmed unchanged as of this session's AWS docs check, but
it's the one line item AWS has changed before), immediately after the runbook, before `## Key Design
Decisions`.

- [ ] **Step 3: Final full-repo Terraform verification**

```bash
cd terraform
terraform fmt -check
terraform validate
terraform plan -out=/dev/null
```
Expected: `Success!` end to end across every `.tf` file from Tasks 1-6.

- [ ] **Step 4: Final full pytest + lint pass**

Using the Linux container recipe from Phase 4a's plan (Global Constraints there):
```bash
ruff check . && pytest -v
```
Expected: full suite passes, including `tests/test_cloud_deployment.py` (Phase 4a) and
`tests/test_deploy_workflow.py` (this plan's Task 5).

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "Document Phase 4 AWS deployment runbook and cost estimate"
```

- [ ] **Step 6: Stop — do not run `terraform apply`**

Per Global Constraints, this plan's execution ends here. Actually standing up the infrastructure
(`terraform apply`, the bootstrap steps, `deploy.yml`) requires explicit user check-in — it spends
real money and creates resources outside this machine. Report the plan as implemented and ask before
proceeding to any of Task 7 Step 1's runbook commands for real.

---

## Self-Review

**Spec coverage:** §4 (Task 1) ✅. §5 (Task 3) ✅. §6 (Task 7's runbook) ✅. §7 (Task 4) ✅. §8 (Task 2's
logging.tf + Task 4's env vars) ✅. §9 (Task 2's secrets.tf + Task 4's `secrets` blocks) ✅. §10 (Tasks
1, 4, 6 collectively — every named file created) ✅. §11 (Task 5) ✅. §12 (Task 7) ✅. §13 row 6 (Task
5's `tests/test_deploy_workflow.py`) ✅ — rows 1-5 are Phase 4a's, already covered there.

**Placeholder scan:** no TBD/TODO. Two intentional "verify against current docs before writing"
flags (Task 4's provider-argument-name caveat in Global Constraints; Task 5's OIDC thumbprint) are
not placeholders — they're explicit instructions to check a specific, named external source
(`registry.terraform.io/providers/hashicorp/aws/latest/docs`) rather than an unresolved "figure this
out later." No "add appropriate error handling" or unscoped "handle edge cases."

**Type/interface consistency:** every ARN/endpoint reference in Task 4's tables (`aws_db_instance.main.master_user_secret[0].secret_arn`,
`aws_secretsmanager_secret.app_secrets.arn`, `aws_secretsmanager_secret.airflow_db_conn.arn`,
`aws_s3_bucket.main.arn`/`.bucket`) resolves to a resource defined in an earlier task (Tasks 1-3), and
Task 6's outputs re-reference the same resource/attribute names with no renaming. `deploy.yml`'s
`${ECS_CLUSTER}-webserver`/`-scheduler` service-name convention is flagged in Task 5 as needing to
match whatever `aws_ecs_service.webserver`/`scheduler`'s actual `name` argument ends up being in Task
4 — called out explicitly rather than silently assumed identical.
