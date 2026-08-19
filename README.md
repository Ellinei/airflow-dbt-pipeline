# Airflow + dbt + PostgreSQL — Local Data Pipeline

A portfolio-grade data engineering project that wires together **Apache Airflow 2.9**, **dbt-core 1.8**, and **PostgreSQL 15** entirely inside Docker.

Two pipelines run side by side on the same infrastructure:

1. **Toy demo** (below) — a tiny, always-works showcase of the Airflow → dbt → Postgres wiring itself (10-15 rows).
2. **[Real-world Olist e-commerce pipeline](#real-world-data-pipeline-olist)** — the same wiring proven against ~100k real, anonymized orders, with its own ingestion task, dbt models, governance, and MLOps training run.

---

## Architecture

```
┌───────────────────────────────────────────────────────────────┐
│  Docker network                                               │
│                                                               │
│  ┌─────────────────┐     metadata      ┌──────────────────┐   │
│  │ Airflow         │ ◄──────────────►  │ postgres_airflow │   │
│  │  · webserver    │    (port 5432)    │ (Airflow DB)     │   │
│  │  · scheduler    │                   └──────────────────┘   │
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

| Service               | Purpose                                                              | Dev port | Prod port | Env var |
|-----------------------|-----------------------------------------------------------------------|----------|-----------|---------|
| `airflow-webserver`   | Airflow UI                                                           | 8080     | 8090      | `AIRFLOW_WEBSERVER_PORT` |
| `airflow-scheduler`   | DAG scheduling                                                       | —        | —         | — |
| `airflow-init`        | One-shot DB migration + admin user                                   | —        | —         | — |
| `postgres_airflow`    | Airflow metadata store                                               | 5432     | 5442      | `AIRFLOW_DB_PORT` |
| `postgres_warehouse`  | dbt target / data warehouse                                          | 5433     | 5443      | `WAREHOUSE_DB_PORT` |
| `mlflow-server`       | MLflow tracking UI + artifact store (`--profile mlops` in dev; always-on in prod) | 5000     | 5010      | `MLFLOW_PORT` |

---

## Running dev and prod side by side

A second "prod" stack can run at the same time as dev, on the same machine, using the same
`docker-compose.yml` — every difference between them is an env var, not a second compose file.

```bash
# Dev (default — unchanged, no -p flag, keeps existing containers/volumes)
docker compose up -d

# Prod (side by side — separate project name + separate env file)
docker compose -p airflow-dbt-prod --env-file .env.prod up -d
```

`mlflow-server` auto-starts in the prod stack via `COMPOSE_PROFILES=prod-core` in `.env.prod` — no
extra `--profile` flag needed on the command line (dev still requires `--profile mlops` to start it
manually).

> **Shared between both stacks:** host directories (`./dags`, `./logs`, `./dbt_project`, `./data`)
> are mounted into *both* stacks, and the Docker image tag they build/run from is also shared —
> containers, volumes, and ports are the only things isolated per project. Concretely:
> - **`./logs`** is written by both stacks' schedulers unconditionally, from the moment both stacks
>   are up — no DAG needs to run. Both append to
>   `logs/dag_processor_manager/dag_processor_manager.log` and write into
>   `logs/scheduler/<date>/` continuously. Because both stacks also share `./dags` (identical DAG
>   definitions) and both run `@daily`, an unpaused DAG in both stacks produces the *same* `run_id`,
>   so task logs land at the *same* path (`logs/dag_id=…/run_id=scheduled__…/task_id=…/attempt=1.log`)
>   — prod's UI could end up serving a file dev also wrote.
> - **`dbt_project/target/`/`dbt_packages/`** race if `dbt_pipeline` is triggered in both stacks at
>   the same moment — avoid running it in both at once.
> - **The image tag `airflow-dbt-pipeline:latest`** (`x-airflow-common.image` in
>   `docker-compose.yml`) is daemon-global, not project-scoped — rebuilding from either stack retags
>   what the other picks up on its next container recreate. This never actually breaks anything
>   (identical Dockerfile, shared `./dags` guarantees identical code either way) but it is a shared
>   resource, not an isolated one.
>
> **Container renaming:** this phase also removed `container_name:` from dev's core services, so the
> next `docker compose up -d` you run recreates dev's containers under Compose's auto-generated
> names (e.g. `postgres_airflow` → `airflowdbtpipeline-postgres_airflow-1`). Data is unaffected —
> named volumes are unchanged, only container identity changes — but any personal script or alias
> that calls `docker exec -it postgres_warehouse ...` (or similar literal names) will break with "No
> such container" until updated to `docker compose exec <service-name>` (see "Run dbt manually
> inside the scheduler container" below).

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

## Real-World Data Pipeline (Olist)

Proves the same Airflow + dbt + Postgres wiring at real scale, using the Kaggle **"Brazilian E-Commerce Public Dataset by Olist"** (`olistbr/brazilian-ecommerce`) — ~100k real, anonymized orders across 9 raw tables. Runs entirely alongside the toy demo above; nothing in it was modified.

```
data/olist/*.csv (user-downloaded, gitignored)
        │  bind-mounted read-only: ./data → /opt/airflow/data
        ▼
Airflow task `ingest_olist` (pandas + SQLAlchemy)
        │  loads into literal Postgres schema `raw` (raw.olist_*)
        ▼
dbt source('olist_raw', ...)
        │
   models/olist_staging/*.sql  (9 views, schema public_olist_staging)
        │
   models/olist_marts/*.sql    (2 tables, schema public_olist_marts)
        │
   ├─► mart_olist_customer_orders   (one row per customer, grouped by
        │                            customer_unique_id — Olist mints a
        │                            new customer_id per order)
        └─► mart_olist_seller_performance (one row per seller: revenue,
                                            freight, review scores)
```

### Models

| Layer   | Model                              | Materialisation | Description |
|---------|-------------------------------------|-----------------|--------------|
| Staging | `stg_olist_customers`               | view | Customer records (order-scoped `customer_id` + the real repeat-customer `customer_unique_id`) |
| Staging | `stg_olist_orders`                  | view | Order records, status + timestamps |
| Staging | `stg_olist_order_items`             | view | Line items — product, seller, price, freight |
| Staging | `stg_olist_order_payments`          | view | Payment method/value per order |
| Staging | `stg_olist_order_reviews`           | view | Customer review scores + comments |
| Staging | `stg_olist_products`                | view | Product catalog (fixes upstream column-name typos) |
| Staging | `stg_olist_sellers`                 | view | Seller records |
| Staging | `stg_olist_geolocation`             | view | Zip-code geolocation, aggregated to one row per prefix (raw table has ~1M near-duplicate rows) |
| Staging | `stg_olist_product_category_translation` | view | Portuguese → English category names |
| Marts   | `mart_olist_customer_orders`        | table | Per-customer lifetime value, order counts, delivered/cancelled breakdown |
| Marts   | `mart_olist_seller_performance`     | table | Per-seller revenue, freight, average review score |

43 dbt tests cover primary keys, foreign-key relationships, and value ranges/vocabularies (`order_status`, `payment_type`, `review_score`).

### Governance

The same `engineer`/`analyst` role split from the toy demo (`governance/setup_roles.sql`) extends to the new schemas: `engineer` has full read access; `analyst` gets a direct grant on both marts (Olist's customer table carries no name/email, so no masking is needed) plus row-level security on `mart_olist_customer_orders` restricting analysts to delivered orders only.

### MLOps

`mlflow_training_olist` DAG (weekly, `--profile mlops`) trains a `RandomForestRegressor` on `mart_olist_customer_orders` to predict customer lifetime value — the same pattern as the toy demo's `mlflow_training` DAG, but against real signal instead of ~10 rows.

### Setup

1. Download the dataset from Kaggle (`olistbr/brazilian-ecommerce`) — see `data/olist/README.md` for the exact 9 filenames expected.
2. Place all 9 CSVs directly in `./data/olist/` (gitignored — never commit them).
3. `docker compose up -d --build`, then trigger `dbt_pipeline` — `ingest_olist` loads the raw tables, then Cosmos auto-discovers and runs the new dbt models alongside the toy demo's.

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (with Compose v2)
- ~4 GB RAM allocated to Docker

---

## Secrets & Configuration

Copy `.env.example` to `.env` and fill in real values before starting the stack — `.env` is
gitignored and never committed. `.env.example` documents every variable, including how to
generate a fresh Airflow Fernet key.

> Older commits in this repo's history contain local-dev-only demo credentials (Airflow Fernet
> key, default admin password, OpenMetadata service passwords) that predate this file. They have
> since been rotated; current credentials live only in your own `.env`.

For the optional prod stack, copy `.env.prod.example` to `.env.prod` and fill in **independently
generated** secrets — never copy values from `.env`. `.env.prod` is gitignored the same way `.env`
is.

If you use the optional `catalog` profile (OpenMetadata), the ingestion CLI configs are
generated from tracked templates rather than committed directly:
```bash
envsubst < openmetadata/postgres_ingestion.yaml.template > openmetadata/postgres_ingestion.yaml
envsubst < openmetadata/dbt_ingestion.yaml.template > openmetadata/dbt_ingestion.yaml
```

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
#    http://localhost:8080   login: admin / $AIRFLOW_ADMIN_PASSWORD (see .env)

# 5. Trigger the DAG manually from the UI, or wait for the daily schedule
```

### Prod stack (optional, runs alongside dev)

```bash
# 1. Copy the prod template and fill in independently-generated secrets
cp .env.prod.example .env.prod

# 2. Start the prod stack under its own Compose project name
docker compose -p airflow-dbt-prod --env-file .env.prod up -d

# 3. Bootstrap the engineer/analyst roles against the NEW prod warehouse — every
#    dbt model has a grant_select() post-hook that requires these roles to
#    exist, and prod's warehouse is a fresh, independent database that never
#    inherits dev's roles. Skipping this step fails dbt_pipeline's very first
#    run in prod with "role engineer does not exist".
docker compose -p airflow-dbt-prod --env-file .env.prod exec -T postgres_warehouse \
  psql -U warehouse -d warehouse \
  -v engineer_password="$GOVERNANCE_ENGINEER_PASSWORD" \
  -v analyst_password="$GOVERNANCE_ANALYST_PASSWORD" \
  < governance/setup_roles.sql

# 4. Open the prod Airflow UI
#    http://localhost:8090   login: admin / $AIRFLOW_ADMIN_PASSWORD (see .env.prod)
```

See "Running dev and prod side by side" above for the shared-resource and container-renaming caveats.

### Run dbt manually inside the scheduler container

```bash
docker compose exec airflow-scheduler bash
# Prod stack equivalent:
docker compose -p airflow-dbt-prod --env-file .env.prod exec airflow-scheduler bash
```

Inside the container:
```bash
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
Database: $WAREHOUSE_DB_NAME  (see .env — default "warehouse")
User:     $WAREHOUSE_DB_USER  (see .env — default "warehouse")
Password: $WAREHOUSE_DB_PASSWORD  (see .env)
```

(Prod stack: port 5443, see `.env.prod`.)

---

## Project Structure

```
.
├── .env                          # Credentials (never commit to git)
├── .env.example                  # Template listing every required variable
├── .env.prod                     # Prod stack credentials (never commit to git)
├── .env.prod.example             # Template listing every required prod variable
├── docker-compose.yml            # All services
├── Dockerfile                    # Custom Airflow image (dbt, Cosmos, pandas, mlflow...)
├── dags/
│   ├── dbt_pipeline_dag.py       # Main DAG: ingest_olist + dbt seed/run/test (Cosmos)
│   ├── mlflow_training_dag.py    # Toy-demo MLOps training run
│   ├── mlflow_training_olist_dag.py  # Real-data MLOps training run
│   └── rag_index_dag.py          # Embeds dbt catalog descriptions into pgvector
├── data/
│   └── olist/                    # Real Olist CSVs (gitignored — see README.md there)
├── dbt_project/
│   ├── dbt_project.yml           # dbt project config
│   ├── profiles.yml              # Warehouse connection (reads env vars)
│   ├── seeds/
│   │   ├── raw_customers.csv
│   │   └── raw_orders.csv
│   ├── models/
│   │   ├── staging/              # Toy demo staging (stg_customers, stg_orders)
│   │   ├── marts/                # Toy demo marts + Power BI exposures
│   │   ├── olist_staging/        # 9 Olist staging views + sources.yml
│   │   └── olist_marts/          # 2 Olist marts (customer orders, seller performance)
│   ├── macros/
│   │   └── grant_select.sql      # Post-hook: grants + concurrency-safe schema lock
│   └── tests/                    # Custom singular tests
├── governance/
│   └── setup_roles.sql           # Roles, schema grants, row-level security
├── docs/superpowers/              # Design specs + implementation plans
├── logs/                         # Airflow task logs (auto-populated)
└── plugins/                      # Custom Airflow plugins (empty)
```

---

## Cloud Deployment (AWS)

Phase 4 adds an optional, fully-torn-down-between-sessions AWS deployment on top of the same
Airflow + dbt + Postgres stack described above — a portfolio/demo deployment, not an always-on
production environment. It's optimized for low cost and a clean `terraform destroy`, not scale or
high availability: managed services (RDS, ECS Fargate, ECR, Secrets Manager) provisioned by
Terraform, public subnets with no NAT gateway (security groups lock inbound traffic to the
operator's own IP), and CI/CD deploys that only ever run on manual `workflow_dispatch` — pushing to
`master` never touches AWS or costs money. Full design rationale lives in
[`docs/superpowers/specs/2026-08-18-phase4-cloud-deployment-design.md`](docs/superpowers/specs/2026-08-18-phase4-cloud-deployment-design.md).

### 1. Architecture

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

### 2. Prerequisites

- [AWS CLI](https://aws.amazon.com/cli/) installed and configured (`aws configure`, or an SSO login) with an account able to create the resources above
- [Terraform](https://developer.hashicorp.com/terraform/downloads) `>= 1.5`
- [Docker](https://docs.docker.com/get-docker/) installed and running locally — needed for the
  one-time manual image push in step 5 (subsequent redeploys use `deploy.yml`'s CI-hosted Docker
  instead)
- `psql` (a PostgreSQL client) installed locally and on `PATH` — the bootstrap steps below shell out
  to it directly (not via Docker), four separate invocations across steps 5.2 and 5.3
- An AWS account
- Your current public IPv4 address: `curl ifconfig.me`
- The Olist CSVs already present at `data/olist/*.csv` before you reach step 7 — `data/` is
  gitignored, so a fresh clone won't have them; see [Real-World Data Pipeline
  (Olist)](#real-world-data-pipeline-olist) to download them (or run the local stack's `ingest_olist`
  once) beforehand

### 3. First-time setup

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # fill in operator_ip and github_repo
terraform init
terraform apply
cd ..   # remaining steps run from the repo root; terraform outputs below are read via -chdir=terraform
```

**Troubleshooting: `EntityAlreadyExists` on the OIDC provider.** `terraform/oidc.tf` creates an
`aws_iam_openid_connect_provider` for `token.actions.githubusercontent.com`. AWS allows only **one**
such provider per issuer URL per account, so if this AWS account already has a GitHub OIDC provider
configured (e.g. left over from an unrelated project's CI setup), `terraform apply` above fails with
`EntityAlreadyExists`. Fix: adopt the existing provider into this Terraform state instead of trying to
create a second one, then re-apply — run from `terraform/`:

```bash
terraform import aws_iam_openid_connect_provider.github \
  arn:aws:iam::<account-id>:oidc-provider/token.actions.githubusercontent.com
terraform apply
cd ..   # back to repo root — remaining steps assume this, same as step 3 above
```

In a **shared** AWS account, two extra precautions: first, run `terraform plan` before that `apply`
and read it — `terraform/oidc.tf` pins `client_id_list = ["sts.amazonaws.com"]`, so if the existing
provider was configured with additional audiences another project relies on, applying this config
would silently strip them (add those audiences to `client_id_list` locally before applying if so).
Second, carry this through to teardown (step 11) too: if other repositories or projects depend on the
same OIDC provider, a plain `terraform destroy` would delete it out from under them — run
`terraform state rm aws_iam_openid_connect_provider.github` first (from `terraform/`) so `destroy`
leaves it alone, and re-run the `import` above at the start of your next session, since the state no
longer tracks it.

### 4. Populate the two operator-managed secrets

Must happen after `apply` — both secrets below depend on the RDS master secret already existing:

```bash
# Fernet key — same command .env.example already documents for local dev:
python -c "import base64, secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())"

RDS_MASTER_SECRET=$(terraform -chdir=terraform output -raw rds_master_secret_arn)
RDS_USER=$(aws secretsmanager get-secret-value --secret-id "$RDS_MASTER_SECRET" \
  --query SecretString --output text | python -c "import json,sys; print(json.load(sys.stdin)['username'])")
RDS_PASS=$(aws secretsmanager get-secret-value --secret-id "$RDS_MASTER_SECRET" \
  --query SecretString --output text | python -c "import json,sys; print(json.load(sys.stdin)['password'])")
RDS_ENDPOINT=$(terraform -chdir=terraform output -raw rds_endpoint)   # "host:port"
RDS_HOST="${RDS_ENDPOINT%%:*}"                                        # bare host, for psql -h below
export PGPASSWORD="$RDS_PASS"   # so the psql calls in step 5 don't prompt interactively for it

aws secretsmanager put-secret-value \
  --secret-id "$(terraform -chdir=terraform output -raw app_secrets_arn)" \
  --secret-string "{\"fernet_key\":\"<paste from above>\",\"webserver_secret_key\":\"<generate the same way>\",\"openai_api_key\":\"<your key, or empty string>\",\"slack_webhook_url\":\"<your webhook, or empty string>\"}"

aws secretsmanager put-secret-value \
  --secret-id "$(terraform -chdir=terraform output -raw airflow_db_conn_secret_arn)" \
  --secret-string "postgresql+psycopg2://${RDS_USER}:${RDS_PASS}@${RDS_ENDPOINT}/airflow"
```

### 5. One-time RDS bootstrap

The ECS task definitions pull `${ecr_repository_url}:latest`, but a fresh `terraform apply` leaves
that repo empty. The ECS *services* tolerate this — they retry on every subsequent deploy (see the
spec's Risks note: "First `terraform apply` has no image to pull yet ... Expected and benign") — but
the one-off `run-task` bootstrap in step 5.1 below has no such retry, so push an image manually
first. This is the same build `deploy.yml` (step 8) will later automate; doing it once by hand here
unblocks both the bootstrap task and the ECS services' first launch:

```bash
# --region must match var.aws_region (terraform/variables.tf; default "us-east-1") — update it if
# you changed that variable
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin "$(terraform -chdir=terraform output -raw ecr_repository_url | cut -d/ -f1)"
# --platform linux/amd64: the ECS task defs default to LINUX_X86_64 — required if you're building on
# an ARM host (e.g. Apple Silicon), otherwise ECS can't pull the resulting image
docker build --platform linux/amd64 -t "$(terraform -chdir=terraform output -raw ecr_repository_url):latest" .
docker push "$(terraform -chdir=terraform output -raw ecr_repository_url):latest"
```

Three ordered steps, run once per fresh `terraform apply`:

1. **Airflow metadata migrate + admin user**, via a one-off `aws ecs run-task` against the scheduler
   task definition, into the same subnets/`ecs_sg` as the running services, `assignPublicIp=ENABLED`
   (needed to reach ECR). Subnet IDs and the security group ID aren't exposed as Terraform outputs, so
   look them up by the `Name`/`group-name` tags Terraform assigns them:
   ```bash
   ECS_CLUSTER=$(terraform -chdir=terraform output -raw ecs_cluster_name)
   SUBNET_IDS=$(aws ec2 describe-subnets \
     --filters "Name=tag:Name,Values=${ECS_CLUSTER}-public-a,${ECS_CLUSTER}-public-b" \
     --query 'Subnets[].SubnetId' --output text | tr '\t' ',')
   SG_ID=$(aws ec2 describe-security-groups \
     --filters "Name=group-name,Values=${ECS_CLUSTER}-ecs-sg" \
     --query 'SecurityGroups[0].GroupId' --output text)

   aws ecs run-task \
     --cluster "$ECS_CLUSTER" \
     --task-definition "${ECS_CLUSTER}-scheduler" \
     --launch-type FARGATE \
     --network-configuration "awsvpcConfiguration={subnets=[$SUBNET_IDS],securityGroups=[$SG_ID],assignPublicIp=ENABLED}" \
     --overrides '{"containerOverrides":[{"name":"scheduler","command":["bash","-c","airflow db migrate && airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password <choose one>"]}]}'
   ```
2. **Warehouse database + pgvector**, from the operator's IP (paths below are repo-root relative —
   run from the repo root, as step 3 leaves you):
   ```bash
   psql -h "$RDS_HOST" -U "$RDS_USER" -d airflow -c "CREATE DATABASE warehouse;"
   psql -h "$RDS_HOST" -U "$RDS_USER" -d warehouse -f warehouse-init/01_pgvector.sql
   psql -h "$RDS_HOST" -U "$RDS_USER" -d warehouse -f warehouse-init/02_catalog_embeddings_unique_index.sql
   ```
3. **Governance roles:**
   ```bash
   export GOVERNANCE_ENGINEER_PASSWORD="<choose a password>"
   export GOVERNANCE_ANALYST_PASSWORD="<choose a password>"
   psql -h "$RDS_HOST" -U "$RDS_USER" -d warehouse \
     -v engineer_password="$GOVERNANCE_ENGINEER_PASSWORD" \
     -v analyst_password="$GOVERNANCE_ANALYST_PASSWORD" \
     -f governance/setup_roles.sql
   ```

### 6. One-time GitHub setup

Set the `AWS_DEPLOY_ROLE_ARN` repo secret to `terraform -chdir=terraform output -raw github_deploy_role_arn`.

### 7. Upload the Olist data once

```bash
aws s3 sync data/olist/ s3://$(terraform -chdir=terraform output -raw s3_bucket_name)/olist-raw/
```

### 8. Deploy

Run the `deploy.yml` workflow manually — GitHub Actions UI ("Run workflow"), or:

```bash
gh workflow run deploy.yml
```

### 9. Find the webserver's public IP

There's no stable URL without an Application Load Balancer (out of scope — see the spec's Risks
section), so look it up after each deploy:

```bash
ECS_CLUSTER=$(terraform -chdir=terraform output -raw ecs_cluster_name)
TASK_ARN=$(aws ecs list-tasks --cluster "$ECS_CLUSTER" \
  --service-name "${ECS_CLUSTER}-webserver" --query 'taskArns[0]' --output text)
ENI_ID=$(aws ecs describe-tasks --cluster "$ECS_CLUSTER" --tasks "$TASK_ARN" \
  --query 'tasks[0].attachments[0].details[?name==`networkInterfaceId`].value' --output text)
aws ec2 describe-network-interfaces --network-interface-ids "$ENI_ID" \
  --query 'NetworkInterfaces[0].Association.PublicIp' --output text
```

Note: `AIRFLOW__WEBSERVER__BASE_URL` isn't set anywhere in this deployment, so any links Airflow
generates itself (e.g. in Slack alert messages) will point at `localhost` rather than the public IP
above. This is an expected consequence of the no-ALB/no-stable-URL design (see the spec's Risks
section) — there's no fixed hostname to set `BASE_URL` to in the first place — not a bug to fix.

### 10. Log in and trigger the pipeline

Log in at `http://<public-ip>:8080` with the admin user created in step 5, unpause `dbt_pipeline`
(it lands **paused** on first deploy, per the existing `DAGS_ARE_PAUSED_AT_CREATION=true` config —
easy to mistake for a broken deploy if you don't expect it), trigger it, and confirm task logs
render in the UI (proves remote S3 task logging is wired up correctly between the two separate
Fargate containers).

### 11. Teardown

In a **shared** AWS account where other repositories/projects depend on the same GitHub OIDC provider
(step 3's troubleshooting note), run this first so `destroy` below doesn't remove it out from under
them — skip this in a dedicated/solo account, since it just means re-importing it next session for no
benefit:

```bash
terraform -chdir=terraform state rm aws_iam_openid_connect_provider.github
```

Then, in all cases:

```bash
terraform -chdir=terraform destroy
```

Full teardown between demo sessions — $0 standing cost. `destroy` removes the ECR repo along with
everything else, so the next `apply` starts from empty again. The accepted tradeoff is redoing steps
4, 5 (including its manual image-push lead-in — the old image is gone with the old repo), 7, and 8
(~10-15 min, plus image build/push time — the image is ~3–4 GB, see the Cost estimate's ECR row)
before the next session: 4 (secrets — the RDS master secret is regenerated by the new
`apply`), 5 (RDS bootstrap, image push included — new database, no migrate/admin-user/pgvector/
governance state survives), 7 (Olist re-upload — new S3 bucket), and 8 (deploy — re-pushes the image
via CI so later redeploys go back to being one command). Step 6 (GitHub setup) only needs redoing if
the deploy role's ARN changed. In a **shared** AWS account where you `state rm`'d the OIDC provider
before this `destroy` (see step 3's troubleshooting note), also redo the `terraform import` at the
start of the next session — the provider itself is untouched, but this project's state no longer
knows about it.

### Cost estimate

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

---

## Key Design Decisions

| Decision | Rationale |
|---|---|
| `_PIP_ADDITIONAL_REQUIREMENTS` | Simple for local dev; no custom Dockerfile needed. In production, pre-bake packages into a `Dockerfile` to avoid 2-3 min install on every container restart. |
| Two PostgreSQL containers | Mirrors real-world separation of Airflow metadata and the analytics warehouse — same pattern as Snowflake + managed Airflow in production. |
| Cosmos `LoadMode.DBT_LS` | Cosmos calls `dbt ls` at scheduler startup to auto-discover all models. You get one Airflow task per node without hard-coding anything. |
| File-based `profiles.yml` | Avoids needing an Airflow Connection object for the warehouse — credentials flow from `.env` → docker-compose env vars → dbt `env_var()`. |
| Staging as VIEWs, Marts as TABLEs | Standard dbt layering: staging is cheap to rebuild, marts are materialised for BI performance. |
| One shared `docker-compose.yml` for dev + prod | Every dev/prod difference is a scalar env-var value (ports, `DBT_TARGET`, `COMPOSE_PROFILES`), not structure — avoids double-maintaining service definitions across two files. Trade-off: host bind mounts (`./dags`, `./logs`, `./dbt_project`, etc.) are shared between both stacks, so avoid running `dbt_pipeline` in both at once. |

---

## Stopping / Resetting

```bash
# Stop containers (keeps data volumes)
docker compose down

# Full reset — removes ALL data including the warehouse
docker compose down -v

# Prod stack equivalents
docker compose -p airflow-dbt-prod --env-file .env.prod down
docker compose -p airflow-dbt-prod --env-file .env.prod down -v
```
