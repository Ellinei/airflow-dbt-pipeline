# ── RDS subnet group (two AZs — required by aws_db_subnet_group even for a
#    single-AZ instance) ──────────────────────────────────────────────────────

resource "aws_db_subnet_group" "main" {
  name       = "${var.project_name}-db-subnet-group"
  subnet_ids = [aws_subnet.public_a.id, aws_subnet.public_b.id]

  tags = {
    Name = "${var.project_name}-db-subnet-group"
  }
}

# ── RDS PostgreSQL instance ───────────────────────────────────────────────
# Provisions exactly one initial database ("airflow", for Airflow's metadata).
# The second database ("warehouse", for dbt/pgvector) is created manually as
# part of the one-time bootstrap (Task 7) — aws_db_instance has no argument
# for provisioning a second database.

resource "aws_db_instance" "main" {
  engine         = "postgres"
  engine_version = "15" # resolves to the latest available 15.x automatically; pgvector requires >= 15.2

  instance_class    = var.db_instance_class
  allocated_storage = 20
  storage_type      = "gp3"
  storage_encrypted = true # default AWS-managed KMS key, no extra cost; must be set pre-apply — enabling later forces replacement

  db_name  = "airflow"
  username = var.db_master_username

  # RDS creates and manages the master credential directly in Secrets
  # Manager; no `password` argument — mutually exclusive with this.
  manage_master_user_password = true

  publicly_accessible    = true
  vpc_security_group_ids = [aws_security_group.rds_sg.id]
  db_subnet_group_name   = aws_db_subnet_group.main.name

  # Destroy-safety: required, not optional, so `terraform destroy` completes
  # cleanly with no orphaned snapshot.
  multi_az                = false
  backup_retention_period = 0
  skip_final_snapshot     = true
  deletion_protection     = false

  tags = {
    Name = "${var.project_name}-rds"
  }
}
