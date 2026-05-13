# Aurora Serverless v2 PostgreSQL. min_capacity = 0 (auto-pause) so the
# DB pauses when the dispatcher is idle — Stages A+B+C in the engine
# eliminated the per-tick storage queries that previously kept the
# cluster warm.

resource "aws_db_subnet_group" "this" {
  name        = "${local.name}-db"
  description = "Private subnets for ${local.name} Aurora cluster"
  subnet_ids  = aws_subnet.private[*].id

  tags = { Name = "${local.name}-db" }
}

resource "random_password" "db" {
  length  = 32
  special = false # avoid escaping pain when injected as env var
}

resource "aws_secretsmanager_secret" "db_password" {
  name        = "${local.name}/db-password"
  description = "Master password for ${local.name} Aurora cluster"
}

resource "aws_secretsmanager_secret_version" "db_password" {
  secret_id     = aws_secretsmanager_secret.db_password.id
  secret_string = random_password.db.result
}

resource "aws_rds_cluster" "this" {
  cluster_identifier              = "${local.name}-aurora"
  engine                          = "aurora-postgresql"
  engine_mode                     = "provisioned"
  engine_version                  = var.aurora_engine_version
  database_name                   = "orch"
  master_username                 = "orch"
  master_password                 = random_password.db.result
  db_subnet_group_name            = aws_db_subnet_group.this.name
  vpc_security_group_ids          = [aws_security_group.aurora.id]
  storage_encrypted               = true
  backup_retention_period         = 1 # personal-use; bump for production
  skip_final_snapshot             = true
  apply_immediately               = true
  enable_http_endpoint            = false

  serverlessv2_scaling_configuration {
    min_capacity = var.aurora_min_capacity
    max_capacity = var.aurora_max_capacity
  }

  lifecycle {
    # Prevent terraform from rotating the password on every apply when
    # it's read from Secrets Manager. Rotate via Secrets Manager directly
    # if needed.
    ignore_changes = [master_password]
  }
}

resource "aws_rds_cluster_instance" "writer" {
  identifier          = "${local.name}-aurora-writer"
  cluster_identifier  = aws_rds_cluster.this.id
  instance_class      = "db.serverless"
  engine              = aws_rds_cluster.this.engine
  engine_version      = aws_rds_cluster.this.engine_version
  publicly_accessible = false
}

# Fully-constructed Postgres connection URL. Stored as a separate secret
# (rather than reconstructed at task-start time) so the ECS task
# definition can pull it directly into `ORCH_STORAGE__DATABASE_URL`.
# The password's character class is restricted to alphanumeric
# (`random_password.db.special = false`) so the URL needs no escaping.
locals {
  database_url = "postgres://${aws_rds_cluster.this.master_username}:${random_password.db.result}@${aws_rds_cluster.this.endpoint}:${aws_rds_cluster.this.port}/${aws_rds_cluster.this.database_name}"
}

resource "aws_secretsmanager_secret" "database_url" {
  name        = "${local.name}/database-url"
  description = "Constructed Postgres connection URL for orchestrator-app"
}

resource "aws_secretsmanager_secret_version" "database_url" {
  secret_id     = aws_secretsmanager_secret.database_url.id
  secret_string = local.database_url
}
