
# ─────────────────────────────────────────────────────────────────────────────
# Root Module — Orchestrates All Infrastructure
# ─────────────────────────────────────────────────────────────────────────────

# ─── S3 Data Lake ────────────────────────────────────────────────────────────

module "s3_data_lake" {
  source = "./modules/s3_data_lake"

  name_prefix             = local.name_prefix
  bucket_names            = local.bucket_names
  landing_expiration_days = var.landing_expiration_days
  bronze_transition_days  = var.bronze_transition_days
  enable_glacier_archival = var.enable_glacier_archival
}

# ─── IAM Roles & Policies ───────────────────────────────────────────────────

module "iam" {
  source = "./modules/iam"

  name_prefix    = local.name_prefix
  bucket_arns    = module.s3_data_lake.bucket_arns
  landing_bucket = module.s3_data_lake.bucket_arns["landing"]
  bronze_bucket  = module.s3_data_lake.bucket_arns["bronze"]
  silver_bucket  = module.s3_data_lake.bucket_arns["silver"]
  gold_bucket    = module.s3_data_lake.bucket_arns["gold"]
  quarantine_bucket = module.s3_data_lake.bucket_arns["quarantine"]
}

# ─── Networking ──────────────────────────────────────────────────────────────

module "networking" {
  source = "./modules/networking"

  name_prefix          = local.name_prefix
  vpc_cidr             = var.vpc_cidr
  availability_zones   = var.availability_zones
  private_subnet_cidrs = var.private_subnet_cidrs
  public_subnet_cidrs  = var.public_subnet_cidrs
}

# ─── Monitoring & Cost Controls ──────────────────────────────────────────────

module "monitoring" {
  source = "./modules/monitoring"

  name_prefix          = local.name_prefix
  alert_email          = var.alert_email
  monthly_budget_limit = var.monthly_budget_limit
  bucket_names         = module.s3_data_lake.bucket_names
}

