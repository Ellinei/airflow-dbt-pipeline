# ── Shared container environment/secrets (webserver + scheduler) ────────────
# One local list of each, referenced from both task definitions below, so
# AIRFLOW__CORE__FERNET_KEY and AIRFLOW__WEBSERVER__SECRET_KEY are guaranteed
# byte-identical on both containers (spec §7) — never hand-duplicated as two
# independently-typed literals.

locals {
  common_environment = [
    { name = "PYTHONPATH", value = "/opt/airflow" },
    { name = "AIRFLOW__CORE__EXECUTOR", value = "LocalExecutor" },
    { name = "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION", value = "true" },
    { name = "AIRFLOW__CORE__LOAD_EXAMPLES", value = "false" },
    { name = "AIRFLOW__API__AUTH_BACKENDS", value = "airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session" },
    { name = "WAREHOUSE_DB_HOST", value = aws_db_instance.main.address },
    { name = "WAREHOUSE_DB_PORT", value = tostring(aws_db_instance.main.port) },
    { name = "WAREHOUSE_DB_NAME", value = "warehouse" },
    { name = "DBT_TARGET", value = "prod" },
    { name = "OLIST_S3_BUCKET", value = aws_s3_bucket.main.bucket },
    { name = "OPENLINEAGE_DISABLED", value = "true" },
    { name = "AIRFLOW__LOGGING__REMOTE_LOGGING", value = "True" },
    { name = "AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER", value = "s3://${aws_s3_bucket.main.bucket}/task-logs" },
    { name = "AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID", value = "aws_default" },
    # Intentionally empty AWS connection URI — falls through to the task
    # role's credentials via boto3's default credential chain (spec §8).
    { name = "AIRFLOW_CONN_AWS_DEFAULT", value = "aws://" },
  ]

  common_secrets = [
    { name = "WAREHOUSE_DB_USER", valueFrom = "${aws_db_instance.main.master_user_secret[0].secret_arn}:username::" },
    { name = "WAREHOUSE_DB_PASSWORD", valueFrom = "${aws_db_instance.main.master_user_secret[0].secret_arn}:password::" },
    # Whole-value secret (no `::key::` suffix) — airflow_db_conn holds the
    # full connection URL as a plain string, not a JSON document.
    { name = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", valueFrom = aws_secretsmanager_secret.airflow_db_conn.arn },
    { name = "AIRFLOW__CORE__FERNET_KEY", valueFrom = "${aws_secretsmanager_secret.app_secrets.arn}:fernet_key::" },
    { name = "AIRFLOW__WEBSERVER__SECRET_KEY", valueFrom = "${aws_secretsmanager_secret.app_secrets.arn}:webserver_secret_key::" },
    { name = "OPENAI_API_KEY", valueFrom = "${aws_secretsmanager_secret.app_secrets.arn}:openai_api_key::" },
    { name = "SLACK_WEBHOOK_URL", valueFrom = "${aws_secretsmanager_secret.app_secrets.arn}:slack_webhook_url::" },
  ]
}

# ── ECS cluster ───────────────────────────────────────────────────────────

resource "aws_ecs_cluster" "main" {
  name = var.project_name

  tags = {
    Name = "${var.project_name}-cluster"
  }
}

# ── Task definitions ──────────────────────────────────────────────────────
# Both built from the same ECR image, differing only in container command —
# mirrors how docker-compose.yml already splits these two roles off one
# image.

resource "aws_ecs_task_definition" "webserver" {
  family                   = "${var.project_name}-webserver"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([
    {
      name      = "webserver"
      image     = "${aws_ecr_repository.main.repository_url}:latest"
      command   = ["webserver"]
      essential = true

      portMappings = [
        {
          containerPort = 8080
          protocol      = "tcp"
        }
      ]

      environment = local.common_environment
      secrets     = local.common_secrets

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.webserver.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])

  tags = {
    Name = "${var.project_name}-webserver"
  }
}

resource "aws_ecs_task_definition" "scheduler" {
  family                   = "${var.project_name}-scheduler"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "1024"
  memory                   = "2048"
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([
    {
      name      = "scheduler"
      image     = "${aws_ecr_repository.main.repository_url}:latest"
      command   = ["scheduler"]
      essential = true

      environment = local.common_environment
      secrets     = local.common_secrets

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.scheduler.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])

  tags = {
    Name = "${var.project_name}-scheduler"
  }
}

# ── ECS services ──────────────────────────────────────────────────────────
# No NAT gateway in this VPC (network.tf) — Fargate tasks in a public subnet
# need assign_public_ip = true just to pull the image from ECR, or every
# task fails with CannotPullContainerError.

resource "aws_ecs_service" "webserver" {
  name            = "${var.project_name}-webserver"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.webserver.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = [aws_subnet.public_a.id, aws_subnet.public_b.id]
    security_groups  = [aws_security_group.ecs_sg.id]
    assign_public_ip = true
  }

  tags = {
    Name = "${var.project_name}-webserver-service"
  }
}

resource "aws_ecs_service" "scheduler" {
  name            = "${var.project_name}-scheduler"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.scheduler.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = [aws_subnet.public_a.id, aws_subnet.public_b.id]
    security_groups  = [aws_security_group.ecs_sg.id]
    assign_public_ip = true
  }

  tags = {
    Name = "${var.project_name}-scheduler-service"
  }
}
