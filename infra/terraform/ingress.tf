# API Gateway HTTP API + VPC Link.
#
# Why not an ALB: ALB carries a ~$0.0252/hour baseline ($18/month) just
# to exist — the single biggest cost line item we can avoid for
# personal-use. API Gateway HTTP API has no hourly baseline; you pay
# per request only.
#
# Architecture:
#   GitHub webhook POST → HTTP API → VPC Link → Cloud Map (SRV) → Fargate task port 8080
#
# The Cloud Map service was created in network.tf; tasks register their
# IPs there via the ECS service's service_registries block (compute.tf).

resource "aws_apigatewayv2_api" "this" {
  name          = "${local.name}-http"
  protocol_type = "HTTP"
  description   = "Public ingress for ${local.name} (webhook + healthz)"
}

resource "aws_apigatewayv2_vpc_link" "this" {
  name               = "${local.name}-vpclink"
  subnet_ids         = aws_subnet.private[*].id
  security_group_ids = [aws_security_group.vpclink.id]
}

resource "aws_apigatewayv2_integration" "task" {
  api_id             = aws_apigatewayv2_api.this.id
  integration_type   = "HTTP_PROXY"
  integration_uri    = aws_service_discovery_service.orch.arn
  integration_method = "ANY"
  connection_type    = "VPC_LINK"
  connection_id      = aws_apigatewayv2_vpc_link.this.id

  # API Gateway is opinionated about timeouts. The orchestrator's
  # webhook handler has its own 5-second lookup-retry budget for the
  # open-then-merge race, so 10s leaves headroom without sitting on
  # stuck requests.
  timeout_milliseconds = 10000
}

resource "aws_apigatewayv2_route" "webhook" {
  api_id    = aws_apigatewayv2_api.this.id
  route_key = "POST /webhook"
  target    = "integrations/${aws_apigatewayv2_integration.task.id}"
}

# `/webhook/{proxy+}` is greedy but does NOT cover the bare `/webhook`
# path itself (HTTP API treats `{proxy+}` as a child-segment matcher).
# The route above handles the exact form GitHub actually POSTs to
# (`webhook_url` output is `<api>/webhook`); this one handles operator-
# configured `path_prefix` subpaths and any future `/webhook/<x>` routes
# the github crate adds.
resource "aws_apigatewayv2_route" "webhook_subpath" {
  api_id    = aws_apigatewayv2_api.this.id
  route_key = "POST /webhook/{proxy+}"
  target    = "integrations/${aws_apigatewayv2_integration.task.id}"
}

# /healthz is process-only (Stage C) so it's safe to expose publicly
# — useful for external uptime monitors. Bandwidth/cost is minimal.
resource "aws_apigatewayv2_route" "healthz" {
  api_id    = aws_apigatewayv2_api.this.id
  route_key = "GET /healthz"
  target    = "integrations/${aws_apigatewayv2_integration.task.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.this.id
  name        = "$default"
  auto_deploy = true

  default_route_settings {
    detailed_metrics_enabled = false
    throttling_burst_limit   = 100
    throttling_rate_limit    = 50
  }
}
