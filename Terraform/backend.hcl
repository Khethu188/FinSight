
# ─────────────────────────────────────────────────────────────────────────────
# Remote State Backend (S3 + DynamoDB Locking)
# ─────────────────────────────────────────────────────────────────────────────
# Prerequisites: Run modules/state_backend/ first to create these resources.

terraform {
  backend "s3" {
    bucket         = "fdp-terraform-state"
    key            = "financial-data-platform/terraform.tfstate"
    region         = "af-south-1"
    encrypt        = true
    dynamodb_table = "fdp-terraform-locks"
  }
}

