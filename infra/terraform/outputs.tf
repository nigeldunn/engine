output "webhook_url" {
  description = "Public HTTPS URL to register as the GitHub App's webhook target. Append nothing; the route is `POST /webhook/{proxy+}`."
  value       = "${aws_apigatewayv2_api.this.api_endpoint}/webhook"
}

output "healthz_url" {
  description = "Public /healthz URL for uptime monitors. Returns 200 OK without touching Postgres."
  value       = "${aws_apigatewayv2_api.this.api_endpoint}/healthz"
}

output "tickets_url" {
  description = "Public ingest endpoint. POST a TicketIngested JSON body with `Authorization: Bearer <token>` (from orch/ingest-bearer-token) to trigger a workflow. Body example: {\"ticket\":{\"source\":\"manual\",\"id\":\"ENG-1\"},\"repo\":{\"owner\":\"o\",\"name\":\"r\"},\"base_branch\":\"main\",\"base_sha\":\"<40-hex>\"}."
  value       = "${aws_apigatewayv2_api.this.api_endpoint}/tickets"
}

output "aurora_writer_endpoint" {
  description = "Aurora cluster writer endpoint (internal). Useful when constructing `ORCH_STORAGE__DATABASE_URL` for the operator-managed secret."
  value       = aws_rds_cluster.this.endpoint
}

output "aurora_database" {
  description = "Database name inside the Aurora cluster."
  value       = aws_rds_cluster.this.database_name
}

output "db_password_secret_arn" {
  description = "Secrets Manager ARN holding the Aurora master password (terraform-generated)."
  value       = aws_secretsmanager_secret.db_password.arn
}

output "database_url_secret_arn" {
  description = "Secrets Manager ARN holding the fully-constructed Postgres URL (`postgres://orch:<password>@<endpoint>:5432/orch`). Terraform constructs this from the password + cluster endpoint; the ECS task injects it as ORCH_STORAGE__DATABASE_URL."
  value       = aws_secretsmanager_secret.database_url.arn
}

output "github_app_pem_secret_arn" {
  description = "Secrets Manager ARN where the operator must put the GitHub App PEM string. ECS injects it as ORCH_GITHUB__PRIVATE_KEY__INLINE."
  value       = aws_secretsmanager_secret.github_app_pem.arn
}

output "github_webhook_secret_arn" {
  description = "Secrets Manager ARN where the operator must put the GitHub webhook signing secret. ECS injects it as ORCH_GITHUB__WEBHOOK_SECRET__INLINE."
  value       = aws_secretsmanager_secret.github_webhook_secret.arn
}

output "agent_bearer_token_secret_arn" {
  description = "Secrets Manager ARN where the operator must put the agent-runner bearer token. ECS injects it as ORCH_AGENT_RUNNER__BEARER_TOKEN__INLINE."
  value       = aws_secretsmanager_secret.agent_bearer_token.arn
}

output "ingest_bearer_token_secret_arn" {
  description = "Secrets Manager ARN where the operator must put the ingest endpoint bearer token. Required only if you bind ingest non-loopback inside the container."
  value       = aws_secretsmanager_secret.ingest_bearer_token.arn
}

output "task_log_group" {
  description = "CloudWatch log group for ECS task stdout/stderr."
  value       = aws_cloudwatch_log_group.task.name
}

output "github_actions_deploy_role_arn" {
  description = "IAM role ARN the GitHub Actions deploy workflow assumes via OIDC. Set this as the repo variable `AWS_DEPLOY_ROLE_ARN`."
  value       = aws_iam_role.github_actions_deploy.arn
}

output "ecs_cluster_name" {
  description = "ECS cluster name. Set this as the repo variable `ECS_CLUSTER` (matches the Terraform default `${"$"}{project}-cluster`)."
  value       = aws_ecs_cluster.this.name
}

output "ecs_service_name" {
  description = "ECS service name. Set this as the repo variable `ECS_SERVICE`."
  value       = aws_ecs_service.orch.name
}

output "ecs_task_family" {
  description = "ECS task definition family. Set this as the repo variable `ECS_TASK_FAMILY`."
  value       = aws_ecs_task_definition.orch.family
}

output "ecs_container_name" {
  description = "Container name within the task definition. The deploy workflow needs this to know which container's `image` field to swap. Set as `ECS_CONTAINER_NAME` repo variable."
  value       = local.name
}
