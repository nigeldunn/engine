# Application secrets. Terraform creates the secret resources; the
# operator populates VALUES after `terraform apply` succeeds:
#
#   aws secretsmanager put-secret-value \
#     --secret-id orch/github-app-pem \
#     --secret-string "$(cat /path/to/github-app.pem)"
#
# The ECS task definition pulls these into env vars matching the
# orchestrator's figment config schema (ORCH_<SECTION>__<FIELD>).

resource "aws_secretsmanager_secret" "github_app_pem" {
  name        = "${local.name}/github-app-pem"
  description = "GitHub App private key (PEM) — full multiline string"
}

resource "aws_secretsmanager_secret" "github_webhook_secret" {
  name        = "${local.name}/github-webhook-secret"
  description = "Shared secret used to HMAC-validate incoming GitHub webhooks"
}

resource "aws_secretsmanager_secret" "agent_bearer_token" {
  name        = "${local.name}/agent-bearer-token"
  description = "Bearer token sent on every call to the agent runner service"
}

resource "aws_secretsmanager_secret" "ingest_bearer_token" {
  name        = "${local.name}/ingest-bearer-token"
  description = "Bearer token required by POST /tickets (only used when ingest is bound non-loopback)"
}
