# ── Secrets Manager shells ────────────────────────────────────────────────
# Both secrets are created empty here. Values are populated manually
# post-`apply` (Task 7's runbook) via the AWS console or CLI — never held in
# a Terraform variable or state, so no `aws_secretsmanager_secret_version`
# resource exists for either.

# JSON secret with four keys: fernet_key, webserver_secret_key,
# openai_api_key, slack_webhook_url. Task 4's ECS `secrets` blocks reference
# each key individually via `valueFrom = "<arn>:<key>::"`.
resource "aws_secretsmanager_secret" "app_secrets" {
  name                    = "${var.project_name}-app-secrets"
  recovery_window_in_days = 0 # allows immediate re-`apply` after a `destroy` with the same name

  tags = {
    Name = "${var.project_name}-app-secrets"
  }
}

# Plain-string secret holding the full postgresql+psycopg2://... connection
# URL. Task 4 references it whole (no `::key::` suffix).
resource "aws_secretsmanager_secret" "airflow_db_conn" {
  name                    = "${var.project_name}-airflow-db-conn"
  recovery_window_in_days = 0 # allows immediate re-`apply` after a `destroy` with the same name

  tags = {
    Name = "${var.project_name}-airflow-db-conn"
  }
}
