# Personal-use deployment for orchestrator-app on AWS.
#
# Target shape (matches `docs/AWS_ECS_AURORA_DEPLOYMENT.md`):
#   - ECS Fargate ARM64 single task in public subnets (assignPublicIp,
#     no NAT Gateway) — cheapest viable runtime.
#   - Aurora Serverless v2 PostgreSQL with min_capacity = 0 ACU so the
#     cluster pauses when the dispatcher is idle (Stages A+B+C in code
#     make this achievable).
#   - API Gateway HTTP API + VPC Link + Cloud Map service for a stable
#     webhook URL (no public ALB; ALB's $0.0252/hour baseline is the
#     single biggest line item we can avoid).
#   - Secrets Manager for the GitHub App PEM, webhook secret, and
#     database password. ECS task injects them as env vars matching the
#     `ORCH_*` figment schema.
#
# Not in scope here: DNS/Route 53, ACM certs, log shipping outside
# CloudWatch, multi-region. Wire those in once the personal-use setup
# is live.

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.50"
    }
    # `random_password` is used in database.tf to seed the Aurora
    # master password (stored in Secrets Manager). Declared explicitly
    # so `terraform init` resolves a known-compatible version rather
    # than silently picking the latest.
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
    }
  }
}

locals {
  name = var.project

  # Two-AZ minimum: Aurora subnet groups need >= 2 AZs even for a single
  # writer instance. Public subnets host the Fargate task (assignPublicIp
  # so the task can reach GitHub, the agent service, and other public
  # APIs without a NAT). Private subnets host Aurora — no public route.
  azs = slice(data.aws_availability_zones.available.names, 0, 2)

  public_subnet_cidrs  = ["10.0.0.0/24", "10.0.1.0/24"]
  private_subnet_cidrs = ["10.0.10.0/24", "10.0.11.0/24"]
}

data "aws_availability_zones" "available" {
  state = "available"
}
