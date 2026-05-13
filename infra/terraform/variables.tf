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
  description = "Fully-qualified container image for the ECS task, e.g. `123456789012.dkr.ecr.ap-southeast-2.amazonaws.com/orch:latest`. Build with the repo Dockerfile (`docker buildx build --platform linux/arm64 .`) and push to the ECR repo this module creates BEFORE running `terraform apply` against the ECS service (ECS will fail to pull a non-existent image)."
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
