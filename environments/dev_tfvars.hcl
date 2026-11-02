
# ─────────────────────────────────────────────────────────────────────────────
# Dev Environment Configuration
# ─────────────────────────────────────────────────────────────────────────────

aws_region   = "af-south-1"
project_name = "financial-data-platform"
environment  = "dev"
owner        = "khethukuthula-sabela"

# Networking
vpc_cidr             = "10.0.0.0/16"
availability_zones   = ["af-south-1a", "af-south-1b"]
private_subnet_cidrs = ["10.0.1.0/24", "10.0.2.0/24"]
public_subnet_cidrs  = ["10.0.101.0/24", "10.0.102.0/24"]

# Cost controls (tight for dev)
monthly_budget_limit = 50
alert_email          = "khethukuthula@example.com"  # Replace with your email

# S3 lifecycle (aggressive cleanup for dev)
landing_expiration_days = 14
bronze_transition_days  = 30
enable_glacier_archival = false

