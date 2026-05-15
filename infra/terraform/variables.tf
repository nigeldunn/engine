variable "project" {
  type        = string
  description = "Project name prefix used on every resource (security groups, cluster, log groups, etc.). Keep it short and lower-kebab-case."
  default     = "orch"
}

variable "region" {
  type        = string
  description = "AWS region. The cost analysis in `docs/AWS_ECS_AURORA_DEPLOYMENT.md` assumes ap-southeast-2; pricing differs by region but the shape of the deployment does not."
  default     = "ap-southeast-2"
}

variable "container_image" {
  type        = string
  default     = null
  description = "Optional override for the bootstrap container image. Defaults to `ghcr.io/<github_repo>:latest`. The deploy workflow registers SHA-pinned revisions after the initial apply, so this only affects the very first task-definition revision."
}

variable "github_repo" {
  type        = string
  description = "GitHub repo in `owner/name` form (e.g. `nigeldunn/engine`). Used to (a) construct the default GHCR image URL and (b) scope the OIDC trust policy on the deploy IAM role to this repo. Case-insensitive for GHCR; the OIDC sub claim is case-sensitive against the repo path GitHub uses."
}

variable "github_oidc_branch" {
  type        = string
  default     = "main"
  description = "Branch the OIDC role trusts for deploys. Set to `*` to trust any ref on the repo (less safe, but useful for tag-based deploys or PR-driven workflows)."
}

variable "task_cpu" {
  type        = string
  description = "Fargate CPU units. 256 is the smallest unit and matches the cost analysis."
  default     = "256"
}

variable "task_memory" {
  type        = string
  description = "Fargate memory in MiB. 512 is the smallest combination compatible with 256 CPU on ARM64 Fargate."
  default     = "512"
}

variable "aurora_engine_version" {
  type        = string
  description = "Aurora PostgreSQL engine version. Must be 16.6+ for min_capacity = 0 (auto-pause)."
  default     = "16.6"
}

variable "aurora_min_capacity" {
  type        = number
  description = "Aurora Serverless v2 min ACU. 0 enables auto-pause when no connections are open (the entire point of Stages A+B+C). Bump to 0.5 if cold-resume latency becomes a problem."
  default     = 0
}

variable "aurora_max_capacity" {
  type        = number
  description = "Aurora Serverless v2 max ACU cap. 1.0 is enough for personal-use load; raise if you see throttling under load."
  default     = 1.0
}

variable "webhook_port" {
  type        = number
  description = "Container port the webhook listener binds to. Matches the `[server.webhook].listen` port in the orchestrator config."
  default     = 8080
}

variable "log_retention_days" {
  type        = number
  description = "CloudWatch Logs retention. 14 days is plenty for personal-use debugging without paying for long retention."
  default     = 14
}

variable "github_app_id" {
  type        = number
  description = "GitHub App ID from the App settings page. Visible in task-definition state (not a secret)."
}

variable "github_install_id" {
  type        = number
  description = "Installation ID for the org/user the GitHub App is installed on. Visible in task-definition state (not a secret)."
}

variable "agent_runner_base_url" {
  type        = string
  description = "Base URL the orchestrator hits for `/run/{type}` and `/status/{type}/{id}`. Personal-use deploys without an agent service can leave this as the default placeholder — actions will fail and the sink will be marked unhealthy, but the engine boots."
  default     = "http://agent-svc.invalid:8080"
}
