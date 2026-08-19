# ── GitHub Actions OIDC provider ─────────────────────────────────────────
# No `thumbprint_list` — AWS validates GitHub's OIDC server certificate
# against its own trusted root CA library rather than a caller-supplied
# thumbprint for known providers (GitHub among them), and `thumbprint_list`
# has been an (Optional) argument since terraform-provider-aws v5.81.0
# (this repo is locked to 5.100.0 — see terraform/.terraform.lock.hcl).
# Verified 2026-08-20 against the provider's docs source
# (github.com/hashicorp/terraform-provider-aws, website/docs/r/iam_openid_connect_provider.html.markdown):
# "For certain OIDC identity providers (e.g., Auth0, GitHub, GitLab, Google,
# or those using an Amazon S3-hosted JWKS endpoint), AWS relies on its own
# library of trusted root certificate authorities (CAs) for validation
# instead of using any configured thumbprints." A `tls_certificate` data
# source to compute a thumbprint is therefore unnecessary here.

resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]

  tags = {
    Name = "${var.project_name}-github-oidc"
  }
}

# ── GitHub Actions deploy role ───────────────────────────────────────────
# Trusted only by workflow runs from var.github_repo (any branch/ref, any
# workflow) — no long-lived AWS keys stored as GitHub secrets (spec §11).

data "aws_iam_policy_document" "github_deploy_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repo}:*"]
    }
  }
}

resource "aws_iam_role" "github_deploy" {
  name               = "${var.project_name}-github-deploy"
  assume_role_policy = data.aws_iam_policy_document.github_deploy_assume_role.json

  tags = {
    Name = "${var.project_name}-github-deploy"
  }
}

# ECR push + ECS force-new-deployment only (spec §11) — GetAuthorizationToken
# must use Resource = "*" since that action doesn't support resource-level
# restriction; everything else is scoped to this project's specific
# repository/services.
data "aws_iam_policy_document" "github_deploy" {
  statement {
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  statement {
    effect = "Allow"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:GetDownloadUrlForLayer",
      "ecr:BatchGetImage",
      "ecr:PutImage",
      "ecr:InitiateLayerUpload",
      "ecr:UploadLayerPart",
      "ecr:CompleteLayerUpload",
    ]
    resources = [aws_ecr_repository.main.arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["ecs:UpdateService", "ecs:DescribeServices"]
    resources = [aws_ecs_service.webserver.id, aws_ecs_service.scheduler.id]
  }
}

resource "aws_iam_role_policy" "github_deploy" {
  name   = "${var.project_name}-github-deploy"
  role   = aws_iam_role.github_deploy.id
  policy = data.aws_iam_policy_document.github_deploy.json
}
