
# ─────────────────────────────────────────────────────────────────────────────
# Production Environment Configuration
# ─────────────────────────────────────────────────────────────────────────────

aws_region   = "af-south-1"
project_name = "financial-data-platform"
environment  = "prod"
owner        = "data-engineering"

# Networking
vpc_cidr             = "10.1.0.0/16"
availability_zones   = ["af-south-1a", "af-south-1b"]
private_subnet_cidrs = ["10.1.1.0/24", "10.1.2.0/24"]
public_subnet_cidrs  = ["10.1.101.0/24", "10.1.102.0/24"]

# Cost controls
monthly_budget_limit = 500
alert_email          = "data-engineering-team@example.com"

# S3 lifecycle (retain longer in prod)
landing_expiration_days = 30
bronze_transition_days  = 90
enable_glacier_archival = true

