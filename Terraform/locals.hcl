
# ─────────────────────────────────────────────────────────────────────────────
# Computed Local Values & Tagging Strategy
# ─────────────────────────────────────────────────────────────────────────────

locals {
  # Naming convention: {project}-{environment}-{resource}
  name_prefix = "${var.project_name}-${var.environment}"

  # Common tags applied to ALL resources via provider default_tags
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    Owner       = var.owner
    ManagedBy   = "terraform"
    Repository  = "github.com/Khethu188/financial-data-platform"
    CostCenter  = "data-engineering"
  }

  # S3 bucket names for each Medallion layer
  bucket_names = {
    landing    = "${local.name_prefix}-landing"
    bronze     = "${local.name_prefix}-bronze"
    silver     = "${local.name_prefix}-silver"
    gold       = "${local.name_prefix}-gold"
    quarantine = "${local.name_prefix}-quarantine"
    manifests  = "${local.name_prefix}-manifests"
  }
}

