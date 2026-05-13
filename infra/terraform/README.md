# `infra/terraform` — personal-use AWS deployment

Terraform module that provisions the cheapest viable AWS deployment for
`orchestrator-app`:

- VPC with two AZs (public + private subnets, no NAT Gateway)
- Aurora Serverless v2 PostgreSQL with `min_capacity = 0` ACU (auto-pause)
- ECS Fargate ARM64 task, 0.25 vCPU / 0.5 GB, in public subnets with `assignPublicIp = true`
- API Gateway HTTP API + VPC Link + Cloud Map for stable webhook URL (no ALB)
- Secrets Manager for GitHub App PEM, webhook secret, agent bearer, ingest bearer, DB password
- CloudWatch log group for task stdout/stderr
- ECR repo for the container image

The application-side correctness work that makes the auto-pause cost
profile viable lives upstream in commits `428322d`, `0515353`, and
`980fc70` (see `docs/AWS_ECS_AURORA_DEPLOYMENT.md` status banner).

## Files

| File | What it provisions |
| --- | --- |
| `main.tf` | terraform + provider blocks, locals, `aws_availability_zones` data source |
| `variables.tf` | All inputs (project name, region, image URI, task sizing, Aurora capacity, log retention, webhook port) |
| `network.tf` | VPC, subnets, route tables, IGW, security groups, Cloud Map namespace + service |
| `database.tf` | Aurora cluster + writer instance + DB subnet group + master-password secret |
| `secrets.tf` | Application secrets (GitHub PEM, webhook secret, agent bearer, ingest bearer) — values populated out-of-band |
| `compute.tf` | ECR repo, ECS cluster, IAM roles, log group, task definition, service |
| `ingress.tf` | API Gateway HTTP API, VPC Link, integration, routes (`POST /webhook/{proxy+}`, `GET /healthz`) |
| `outputs.tf` | Webhook URL, ECR URI, secret ARNs the operator needs to populate |

## Bring-up

```sh
# 1. Initialize.
cd infra/terraform
terraform init

# 2. Build + push the container image FIRST. Without it, the ECS
#    service errors out and terraform apply hangs.
ECR_URI=$(terraform output -raw ecr_repository_url 2>/dev/null || echo "see plan")
docker buildx build --platform linux/arm64 -t "${ECR_URI}:latest" ../..
# If ECR_URI isn't known yet (first apply), do this AFTER `terraform apply`
# creates the repo, then re-apply with -var container_image=<uri>.

aws ecr get-login-password --region ap-southeast-2 \
  | docker login --username AWS --password-stdin "${ECR_URI}"
docker push "${ECR_URI}:latest"

# 3. Plan + apply.
terraform apply -var "container_image=${ECR_URI}:latest"

# 4. Populate secrets (the resources exist; the values are placeholders).
aws secretsmanager put-secret-value --secret-id orch/github-app-pem \
  --secret-string "$(cat ~/path/to/github-app.pem)"
aws secretsmanager put-secret-value --secret-id orch/github-webhook-secret \
  --secret-string "<your webhook secret>"
aws secretsmanager put-secret-value --secret-id orch/agent-bearer-token \
  --secret-string "<your agent runner token>"
aws secretsmanager put-secret-value --secret-id orch/ingest-bearer-token \
  --secret-string "<your ingest token>"

# 5. Register the webhook URL with your GitHub App.
terraform output -raw webhook_url
# The DATABASE_URL is fully managed by Terraform: it's constructed from
# the cluster endpoint + the auto-generated password and stored at
# `database_url_secret_arn`. The ECS task definition reads it directly.
# Rotate by `terraform taint random_password.db && terraform apply`,
# then redeploy the task.
```

## What's NOT in this module

- DNS / Route 53 / custom domain on the HTTP API. Use the
  `*.execute-api.<region>.amazonaws.com` URL until you need a vanity domain.
- ACM certificates. Implied by no custom domain.
- Multi-region. Single-region single-instance is the design point.
- Autoscaling. `desired_count = 1` is hard-coded because the engine is
  single-instance (see `PLAN.md`). Bump only after action claiming has
  been tested with `FOR UPDATE SKIP LOCKED`.
- Log shipping to anywhere other than CloudWatch.
- Monitoring / alarms. Add a CloudWatch alarm on ECS service desired
  count != running count once you care about pager coverage.

## Cost expectation

For an idle workload (no incoming webhooks, no CLI ingest), Aurora
should pause within 5 minutes and the Fargate task is the only ongoing
cost (~$8/month for the smallest ARM task in ap-southeast-2). With
modest activity, expect $15–25/month total. Match this against the cost
table in `docs/AWS_ECS_AURORA_DEPLOYMENT.md`.
