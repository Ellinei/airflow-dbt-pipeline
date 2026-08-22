data "aws_caller_identity" "current" {}

# ── S3 bucket (task logs + Olist raw data) ───────────────────────────────────
# One bucket, two prefixes: `task-logs/` (Airflow remote task logging, this
# file) and `olist-raw/` (Task 4a's `_ensure_olist_data_available`, uploaded
# manually by the operator post-`apply`) — not two buckets.

resource "aws_s3_bucket" "main" {
  bucket        = "${var.project_name}-${data.aws_caller_identity.current.account_id}"
  force_destroy = true # required for `terraform destroy` to succeed with objects still in the bucket

  tags = {
    Name = "${var.project_name}-main"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "main" {
  bucket = aws_s3_bucket.main.id

  rule {
    id     = "expire-task-logs"
    status = "Enabled"

    filter {
      prefix = "task-logs/"
    }

    expiration {
      days = 30
    }
  }
}

# ── CloudWatch log groups (ECS container stdout/stderr) ─────────────────────
# Created explicitly so they're tracked in Terraform state — if left to
# ECS's auto-create-on-first-log behavior, they're created outside state and
# `terraform destroy` orphans them.

resource "aws_cloudwatch_log_group" "webserver" {
  name              = "/ecs/airflow-webserver"
  retention_in_days = 7

  tags = {
    Name = "${var.project_name}-webserver-logs"
  }
}

resource "aws_cloudwatch_log_group" "scheduler" {
  name              = "/ecs/airflow-scheduler"
  retention_in_days = 7

  tags = {
    Name = "${var.project_name}-scheduler-logs"
  }
}
