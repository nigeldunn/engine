# `infra/terraform` — personal-use AWS deployment

Terraform module that provisions the cheapest viable AWS deployment for
`orchestrator-app`:

- VPC with two AZs (public + private subnets, no NAT Gateway)
- Aurora Serverless v2 PostgreSQL with `min_capacity = 0` ACU (auto-pause)
- ECS Fargate ARM64 task, 0.25 vCPU / 0.5 GB, in public subnets with `assignPublicIp = true`
- API Gateway HTTP API + VPC Link + Cloud Map for stable webhook URL (no ALB)
- Secrets Manager for GitHub App PEM, webhook secret, agent bearer, ingest bearer, DB password
- CloudWatch log group for task stdout/stderr
- GitHub Actions OIDC trust + a least-privilege deploy role consumed by `.github/workflows/deploy.yml`

Container images live in GHCR (`ghcr.io/<github_repo>`), pushed by
the deploy workflow. No ECR repo is provisioned.

The application-side correctness work that makes the auto-pause cost
profile viable lives upstream in commits `428322d`, `0515353`, and
`980fc70` (see `docs/AWS_ECS_AURORA_DEPLOYMENT.md` status banner).

## Files

| File | What it provisions |
| --- | --- |
| `main.tf` | terraform + provider blocks, locals (incl. `container_image` default), `aws_availability_zones` data source |
| `variables.tf` | All inputs (project name, region, image override, github_repo, task sizing, Aurora capacity, log retention, webhook port) |
| `network.tf` | VPC, subnets, route tables, IGW, security groups, Cloud Map namespace + service |
| `database.tf` | Aurora cluster + writer instance + DB subnet group + master-password secret |
| `secrets.tf` | Application secrets (GitHub PEM, webhook secret, agent bearer, ingest bearer) — values populated out-of-band |
| `compute.tf` | ECS cluster, IAM roles, log group, task definition, service |
| `ingress.tf` | API Gateway HTTP API, VPC Link, integration, routes (`POST /webhook/{proxy+}`, `GET /healthz`) |
| `github_oidc.tf` | OIDC provider + IAM role the GitHub Actions deploy workflow assumes |
| `outputs.tf` | Webhook URL, deploy-role ARN, ECS identifiers, secret ARNs the operator needs to populate |

## Bring-up

> **Why no manual `docker push` first?** The deploy workflow uses the
> repo's `GITHUB_TOKEN` to push to GHCR, and that token can only push
> to packages **owned by Actions in this repo**. A package created by
> a manual `docker push` (using a personal PAT) is owned by the user
> account and isn't linked to the repo, so the next workflow run gets
> a 403. Letting the workflow create the package on first run avoids
> the linkage step. The trade-off is one extra round-trip on first
> bring-up: the workflow's first push lands a *private* package, ECS
> can't pull it, you flip it public, then re-run the workflow.

```sh
# 1. Initialize.
cd infra/terraform
terraform init

# 2. Plan + apply. The ECS service comes up unhealthy because no
#    image exists at `ghcr.io/<repo>:latest` yet — that's expected
#    until the workflow's first run lands one.
GH_REPO=<owner>/<repo>   # e.g. nigeldunn/engine
terraform apply \
  -var "github_repo=${GH_REPO}" \
  -var "github_app_id=<your-app-id>" \
  -var "github_install_id=<your-install-id>"

# 3. Populate the application secrets (resources exist; values are placeholders).
aws secretsmanager put-secret-value --secret-id orch/github-app-pem \
  --secret-string "$(cat ~/path/to/github-app.pem)"
aws secretsmanager put-secret-value --secret-id orch/github-webhook-secret \
  --secret-string "<your webhook secret>"
aws secretsmanager put-secret-value --secret-id orch/agent-bearer-token \
  --secret-string "<your agent runner token>"
aws secretsmanager put-secret-value --secret-id orch/ingest-bearer-token \
  --secret-string "<your ingest token>"

# 4. Wire the deploy workflow. Set these as Actions repo Variables
#    (Settings → Secrets and variables → Actions → Variables):
terraform output -raw github_actions_deploy_role_arn   # → AWS_DEPLOY_ROLE_ARN
echo "ap-southeast-2"                                  # → AWS_REGION
terraform output -raw ecs_cluster_name                 # → ECS_CLUSTER
terraform output -raw ecs_service_name                 # → ECS_SERVICE
terraform output -raw ecs_task_family                  # → ECS_TASK_FAMILY
terraform output -raw ecs_container_name               # → ECS_CONTAINER_NAME

# 5. Trigger the deploy workflow once (Actions tab → Deploy → Run
#    workflow). This run pushes the first image, creating the GHCR
#    package owned by the repo. The deploy step will FAIL at
#    wait-for-service-stability because the package is still private
#    and ECS can't pull anonymously — that's the expected one-time
#    state.

# 6. Flip the package to Public. The settings URL depends on whether
#    the repo owner is a user or an organization:
#      user account: https://github.com/users/<owner>/packages/container/<repo>/settings
#      organization: https://github.com/orgs/<owner>/packages/container/<repo>/settings
#    Scroll to "Danger Zone" → "Change package visibility" → Public.
#    The "Manage Actions access" panel above it will already list this
#    repo with Write — that linkage is what lets future workflow runs
#    push.

# 7. Re-run the deploy workflow. ECS pulls successfully, service goes
#    healthy, wait-for-service-stability passes.

# 8. Register the webhook URL with your GitHub App.
terraform output -raw webhook_url

# 9. Subsequent deploys: push to main (or trigger `Deploy` workflow
#    manually). The workflow builds the ARM64 image, pushes to GHCR
#    tagged `sha-<short>`, registers a new ECS task-def revision
#    pinned to that SHA, and waits for service stability.
#
# The DATABASE_URL is fully managed by Terraform — constructed from
# the cluster endpoint + auto-generated password and stored at
# `database_url_secret_arn`. Rotate via
# `terraform taint random_password.db && terraform apply`, then run
# the deploy workflow to roll the task.
```

### Want to verify the build locally before going to AWS?

The Dockerfile is the same one the workflow uses. You can build (but
don't push) for sanity:

```sh
docker buildx build --platform linux/arm64 -t orch:local --load ../..
```

Don't `docker push` to `ghcr.io/<repo>` from your laptop — see the
note at the top of this section about why that breaks workflow auth.

### OIDC provider already exists in this account?

`aws_iam_openid_connect_provider` for `token.actions.githubusercontent.com`
is account-global. If another stack already created it:

```sh
terraform apply \
  -var "create_github_oidc_provider=false" \
  -var "github_oidc_provider_arn=arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com" \
  ... # other vars
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
