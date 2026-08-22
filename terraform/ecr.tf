# ── ECR repository (application image) ───────────────────────────────────
# Task 5's deploy.yml pushes both `:latest` and `:${{ github.sha }}` tags
# here; task definitions in ecs.tf always reference `:latest`.

resource "aws_ecr_repository" "main" {
  name                 = var.project_name
  image_tag_mutability = "MUTABLE" # `:latest` must be re-pushable on every deploy

  force_delete = true # `terraform destroy` succeeds even if images remain

  tags = {
    Name = "${var.project_name}-ecr"
  }
}
