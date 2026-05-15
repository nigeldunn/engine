# ECS cluster + IAM + task definition + service.
#
# Single-task Fargate ARM64. The container image is pulled from GHCR
# (the repo's GitHub Container Registry namespace). The package must be
# public, or ECS needs `repositoryCredentials` wired to a Secrets
# Manager secret holding `{"username":..,"password":..}`. The
# `.github/workflows/deploy.yml` pipeline owns image builds + pushes
# and registers new task-definition revisions; this Terraform owns the
# bootstrap revision (image tag is `:latest` until the first workflow
# deploy pins a SHA).

resource "aws_ecs_cluster" "this" {
  name = "${local.name}-cluster"

  setting {
    name  = "containerInsights"
    value = "disabled" # personal-use; flip to "enabled" for production telemetry
  }
}

resource "aws_cloudwatch_log_group" "task" {
  name              = "/ecs/${local.name}"
  retention_in_days = var.log_retention_days
}

# ── IAM ──────────────────────────────────────────────────────────────

# Execution role: ECS uses this to write to CloudWatch Logs and inject
# secrets from Secrets Manager into task env vars. (No registry-pull
# permissions needed — GHCR public images are fetched anonymously.)
data "aws_iam_policy_document" "task_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "task_execution" {
  name               = "${local.name}-task-execution"
  assume_role_policy = data.aws_iam_policy_document.task_assume.json
}

resource "aws_iam_role_policy_attachment" "task_execution_managed" {
  role       = aws_iam_role.task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# Allow the execution role to read every secret listed in
# secrets.tf + the DB password from database.tf. Granted as a single
# bundled policy because the task always wants them together.
data "aws_iam_policy_document" "secrets_read" {
  statement {
    actions = ["secretsmanager:GetSecretValue"]
    resources = [
      aws_secretsmanager_secret.db_password.arn,
      aws_secretsmanager_secret.database_url.arn,
      aws_secretsmanager_secret.github_app_pem.arn,
      aws_secretsmanager_secret.github_webhook_secret.arn,
      aws_secretsmanager_secret.agent_bearer_token.arn,
      aws_secretsmanager_secret.ingest_bearer_token.arn,
    ]
  }
}

resource "aws_iam_role_policy" "secrets_read" {
  name   = "${local.name}-secrets-read"
  role   = aws_iam_role.task_execution.id
  policy = data.aws_iam_policy_document.secrets_read.json
}

# Task role: any AWS APIs the container itself calls. The
# orchestrator-app binary today calls zero AWS APIs (it talks to
# GitHub + agent service + Postgres), so this is intentionally empty.
# Keeps the principle of least privilege explicit rather than implicit.
resource "aws_iam_role" "task" {
  name               = "${local.name}-task"
  assume_role_policy = data.aws_iam_policy_document.task_assume.json
}

# ── Task definition ─────────────────────────────────────────────────

resource "aws_ecs_task_definition" "orch" {
  family                   = local.name
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  container_definitions = jsonencode([{
    name      = local.name
    image     = local.container_image
    essential = true

    portMappings = [{
      containerPort = var.webhook_port
      hostPort      = var.webhook_port
      protocol      = "tcp"
    }]

    # Inject secrets as env vars that match the figment config schema
    # (prefix ORCH_, double-underscore as section separator). The
    # baked-in `orchestrator.toml` (Stage F gap-a fix: ships at
    # /etc/orchestrator.toml in the image) carries structural fields
    # with placeholder values; these env vars override every secret +
    # the database URL so nothing sensitive lives in the image or in
    # plain task-definition state.
    secrets = [
      {
        name      = "ORCH_STORAGE__DATABASE_URL"
        valueFrom = aws_secretsmanager_secret.database_url.arn
      },
      {
        name      = "ORCH_GITHUB__PRIVATE_KEY__INLINE"
        valueFrom = aws_secretsmanager_secret.github_app_pem.arn
      },
      {
        name      = "ORCH_GITHUB__WEBHOOK_SECRET__INLINE"
        valueFrom = aws_secretsmanager_secret.github_webhook_secret.arn
      },
      {
        name      = "ORCH_AGENT_RUNNER__BEARER_TOKEN__INLINE"
        valueFrom = aws_secretsmanager_secret.agent_bearer_token.arn
      },
      {
        name      = "ORCH_SERVER__INGEST__BEARER_TOKEN__INLINE"
        valueFrom = aws_secretsmanager_secret.ingest_bearer_token.arn
      },
    ]

    environment = [
      # Non-secret overrides that vary per deployment. github.app_id,
      # install_id, and the agent service base URL are deployment
      # identity, not secrets, so they live as terraform variables and
      # plain env vars (visible in task definition state).
      {
        name  = "ORCH_GITHUB__APP_ID"
        value = tostring(var.github_app_id)
      },
      {
        name  = "ORCH_GITHUB__INSTALL_ID"
        value = tostring(var.github_install_id)
      },
      {
        name  = "ORCH_AGENT_RUNNER__BASE_URL"
        value = var.agent_runner_base_url
      },
      {
        name  = "RUST_LOG"
        value = "info,orchestrator=debug"
      },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.task.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "ecs"
      }
    }

    # Container-level liveness probe via the binary's own `health`
    # subcommand. `CMD` (not `CMD-SHELL`) execs the binary directly —
    # the distroless runtime has no shell, but it does have the
    # orchestrator-app binary, which is its own probe. The subcommand
    # opens TCP to 127.0.0.1:webhook_port, sends a one-shot HTTP GET
    # for /healthz, and exits 0 on 200. It loads no config and contacts
    # no external services, so a missing PEM or paused Aurora cannot
    # mark a healthy main process unhealthy.
    healthCheck = {
      command = [
        "CMD",
        "/usr/local/bin/orchestrator-app",
        "health",
        "--port",
        tostring(var.webhook_port),
      ]
      interval    = 30
      timeout     = 5
      retries     = 3
      startPeriod = 60
    }
  }])
}

# ── Service ─────────────────────────────────────────────────────────

resource "aws_ecs_service" "orch" {
  name            = local.name
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.orch.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  # Public subnets + assignPublicIp so the task can reach GitHub / agent
  # service without a NAT. Aurora is still private (separate subnets).
  network_configuration {
    subnets          = aws_subnet.public[*].id
    security_groups  = [aws_security_group.task.id]
    assign_public_ip = true
  }

  # Register tasks in Cloud Map so API Gateway VPC Link can target them.
  service_registries {
    registry_arn = aws_service_discovery_service.orch.arn
    port         = var.webhook_port
  }

  # ECS prefers force_new_deployment on task-definition updates; for
  # personal-use the default rolling deploy is fine. Setting
  # desired_count=1 means there's a brief drain window where the
  # service is unavailable — Aurora wake makes this short enough that
  # webhook redelivery and the ingest endpoint's idempotency cover the
  # gap.
  lifecycle {
    ignore_changes = [task_definition] # operators may bump the image tag without rerunning terraform
  }

  depends_on = [aws_rds_cluster_instance.writer]
}
