# ── Terraform Outputs ─────────────────────────────────────────────────────
# Task 7's bootstrap runbook and GitHub repository secret setup consume these.

output "ecr_repository_url" {
  description = "ECR repository URL for manual docker push before first deploy.yml run"
  value       = aws_ecr_repository.main.repository_url
}

output "rds_endpoint" {
  description = "RDS endpoint for psql access in Task 7 runbook step 2"
  value       = aws_db_instance.main.endpoint
}

output "rds_master_secret_arn" {
  description = "ARN of the RDS master user secret (Secrets Manager) for password retrieval"
  value       = aws_db_instance.main.master_user_secret[0].secret_arn
}

output "app_secrets_arn" {
  description = "ARN of the app_secrets secret (Secrets Manager) for Task 7 put-secret-value"
  value       = aws_secretsmanager_secret.app_secrets.arn
}

output "airflow_db_conn_secret_arn" {
  description = "ARN of the airflow_db_conn secret (Secrets Manager) for Task 7 put-secret-value"
  value       = aws_secretsmanager_secret.airflow_db_conn.arn
}

output "s3_bucket_name" {
  description = "S3 bucket name for aws s3 sync in Task 7 runbook"
  value       = aws_s3_bucket.main.bucket
}

output "ecs_cluster_name" {
  description = "ECS cluster name for bootstrap run-task and describe-tasks in Task 7 runbook"
  value       = aws_ecs_cluster.main.name
}

output "github_deploy_role_arn" {
  description = "ARN of the GitHub deploy IAM role for AWS_DEPLOY_ROLE_ARN GitHub secret"
  value       = aws_iam_role.github_deploy.arn
}
