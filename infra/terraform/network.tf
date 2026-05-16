# VPC + subnets + security groups + Cloud Map.
#
# Single VPC, 2 AZs. Public subnets host the Fargate task (it needs
# outbound internet to reach GitHub / agent service, so we accept the
# public IP rather than pay for a NAT Gateway). Private subnets host
# Aurora — no Internet route, only reachable from the task SG.

resource "aws_vpc" "this" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "${local.name}-vpc" }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${local.name}-igw" }
}

resource "aws_subnet" "public" {
  count                   = length(local.public_subnet_cidrs)
  vpc_id                  = aws_vpc.this.id
  cidr_block              = local.public_subnet_cidrs[count.index]
  availability_zone       = local.azs[count.index]
  map_public_ip_on_launch = true
  tags                    = { Name = "${local.name}-public-${count.index}" }
}

resource "aws_subnet" "private" {
  count             = length(local.private_subnet_cidrs)
  vpc_id            = aws_vpc.this.id
  cidr_block        = local.private_subnet_cidrs[count.index]
  availability_zone = local.azs[count.index]
  tags              = { Name = "${local.name}-private-${count.index}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = { Name = "${local.name}-public-rt" }
}

resource "aws_route_table_association" "public" {
  count          = length(aws_subnet.public)
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# Private subnets get an empty route table (default local-only). No NAT,
# so Aurora can talk to ECS but nothing in private subnets can reach the
# Internet. That's fine for Aurora; it doesn't need outbound.
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${local.name}-private-rt" }
}

resource "aws_route_table_association" "private" {
  count          = length(aws_subnet.private)
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# ── Security groups ──────────────────────────────────────────────────

# Ingress from API Gateway VPC Link. The VPC Link itself lives in the
# private subnets; this SG fronts it and gates ingress to the task SG.
resource "aws_security_group" "vpclink" {
  name        = "${local.name}-vpclink"
  description = "API Gateway VPC Link to ECS task"
  vpc_id      = aws_vpc.this.id

  egress {
    description = "to-task"
    from_port   = var.webhook_port
    to_port     = var.webhook_port
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }

  tags = { Name = "${local.name}-vpclink" }
}

# Task SG. Inbound only from the VPC Link SG; outbound to everywhere
# (the dispatcher fans out to GitHub, the agent service, and Aurora).
resource "aws_security_group" "task" {
  name        = "${local.name}-task"
  description = "ECS task SG for orchestrator-app"
  vpc_id      = aws_vpc.this.id

  ingress {
    description     = "webhook + healthz from VPC Link"
    from_port       = var.webhook_port
    to_port         = var.webhook_port
    protocol        = "tcp"
    security_groups = [aws_security_group.vpclink.id]
  }

  egress {
    description = "all outbound (GitHub, agent service, Aurora)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name}-task" }
}

# Aurora SG. Inbound 5432 from the task SG only; no outbound needed.
resource "aws_security_group" "aurora" {
  name        = "${local.name}-aurora"
  description = "Aurora cluster SG (Postgres 5432 from task SG)"
  vpc_id      = aws_vpc.this.id

  ingress {
    description     = "postgres from task SG"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.task.id]
  }

  tags = { Name = "${local.name}-aurora" }
}

# ── Cloud Map (for API Gateway VPC Link service discovery) ──────────

resource "aws_service_discovery_private_dns_namespace" "this" {
  name        = "${local.name}.internal"
  description = "Private DNS namespace for orch service discovery"
  vpc         = aws_vpc.this.id
}

resource "aws_service_discovery_service" "orch" {
  name = "app"

  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.this.id

    dns_records {
      ttl  = 10
      type = "SRV"
    }

    routing_policy = "MULTIVALUE"
  }

  health_check_custom_config {
    failure_threshold = 1
  }
}

