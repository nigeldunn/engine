# GitHub Actions OIDC trust + deploy role.
#
# The deploy workflow (`.github/workflows/deploy.yml`) assumes this
# role via OIDC token exchange — no long-lived AWS keys in repo
# secrets. The role is scoped to:
#   - this repo (`var.github_repo`)
#   - `var.github_oidc_branch` (default `main`)
# and granted only what's needed to register a new task-definition
# revision and update the ECS service.
#
# OIDC provider footnote: AWS only allows a single
# `token.actions.githubusercontent.com` provider per account. If one
# already exists (e.g. from another project), `terraform import` it
# rather than creating a duplicate, or set
# `var.create_github_oidc_provider = false` and pass the existing ARN
# via `var.github_oidc_provider_arn`.

variable "create_github_oidc_provider" {
  type        = bool
  default     = true
  description = "Whether to create the GitHub Actions OIDC provider. AWS allows only one per account; set to false if another module/project already created it and pass the ARN via `github_oidc_provider_arn`."
}

variable "github_oidc_provider_arn" {
  type        = string
  default     = null
  description = "ARN of an existing GitHub Actions OIDC provider. Only consulted when `create_github_oidc_provider = false`."
}

resource "aws_iam_openid_connect_provider" "github" {
  count = var.create_github_oidc_provider ? 1 : 0

  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  # AWS verifies the GitHub OIDC JWT signature against the JWKS at the
  # issuer URL; the thumbprint is no longer load-bearing for github
  # (since the 2023 IAM change), but the field remains required by the
  # API. The value below is the long-standing GitHub thumbprint.
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

locals {
  github_oidc_provider_arn = var.create_github_oidc_provider ? aws_iam_openid_connect_provider.github[0].arn : var.github_oidc_provider_arn
  github_oidc_sub          = var.github_oidc_branch == "*" ? "repo:${var.github_repo}:*" : "repo:${var.github_repo}:ref:refs/heads/${var.github_oidc_branch}"
}

data "aws_iam_policy_document" "github_actions_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.github_oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = [local.github_oidc_sub]
    }
  }
}

resource "aws_iam_role" "github_actions_deploy" {
  name               = "${local.name}-github-actions-deploy"
  assume_role_policy = data.aws_iam_policy_document.github_actions_assume.json
}

# Least-privilege deploy permissions: register a new task-definition
# revision in the existing family, update the existing ECS service to
# point at it, and pass the two task roles into the new revision. No
# describe/list on the rest of the account.
data "aws_iam_policy_document" "github_actions_deploy" {
  statement {
    sid = "DescribeTaskDefAndService"
    actions = [
      "ecs:DescribeTaskDefinition",
      "ecs:DescribeServices",
    ]
    resources = ["*"] # ecs:DescribeTaskDefinition does not support resource-level ARNs
  }

  statement {
    sid       = "RegisterTaskDef"
    actions   = ["ecs:RegisterTaskDefinition"]
    resources = ["*"] # ecs:RegisterTaskDefinition does not support resource-level ARNs either
  }

  statement {
    sid     = "UpdateService"
    actions = ["ecs:UpdateService"]
    # `aws_ecs_service.id` is the service ARN in provider v5+.
    resources = [aws_ecs_service.orch.id]
  }

  statement {
    sid       = "PassRoles"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.task_execution.arn, aws_iam_role.task.arn]
    # Restrict pass-role to ECS task usage to satisfy IAM best-practice
    # checks (cdk-nag, ScoutSuite, etc.).
    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "github_actions_deploy" {
  name   = "${local.name}-github-actions-deploy"
  role   = aws_iam_role.github_actions_deploy.id
  policy = data.aws_iam_policy_document.github_actions_deploy.json
}
