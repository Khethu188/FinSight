
# ─────────────────────────────────────────────────────────────────────────────
# S3 Data Lake Module
# Provisions buckets for all Medallion Architecture layers with:
#   - Server-side encryption (AES-256)
#   - Versioning (Silver, Gold)
#   - Public access blocking
#   - Lifecycle policies for cost optimization
#   - Access logging
# ─────────────────────────────────────────────────────────────────────────────

# ─── Access Logging Bucket ───────────────────────────────────────────────────

resource "aws_s3_bucket" "access_logs" {
  bucket        = "${var.name_prefix}-access-logs"
  force_destroy = true

  tags = {
    Layer = "logging"
  }
}

resource "aws_s3_bucket_public_access_block" "access_logs" {
  bucket = aws_s3_bucket.access_logs.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "access_logs" {
  bucket = aws_s3_bucket.access_logs.id

  rule {
    id     = "expire-old-logs"
    status = "Enabled"

    expiration {
      days = 90
    }
  }
}

# ─── Data Lake Buckets ───────────────────────────────────────────────────────

resource "aws_s3_bucket" "data_lake" {
  for_each = var.bucket_names

  bucket        = each.value
  force_destroy = true

  tags = {
    Layer = each.key
  }
}

# ─── Encryption (all buckets) ────────────────────────────────────────────────

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  for_each = aws_s3_bucket.data_lake

  bucket = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# ─── Block Public Access (all buckets) ───────────────────────────────────────

resource "aws_s3_bucket_public_access_block" "data_lake" {
  for_each = aws_s3_bucket.data_lake

  bucket = each.value.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ─── Versioning (Silver and Gold only — protect curated data) ────────────────

resource "aws_s3_bucket_versioning" "versioned" {
  for_each = {
    for k, v in aws_s3_bucket.data_lake : k => v
    if contains(["silver", "gold"], k)
  }

  bucket = each.value.id

  versioning_configuration {
    status = "Enabled"
  }
}

# ─── Access Logging (all data lake buckets) ──────────────────────────────────

resource "aws_s3_bucket_logging" "data_lake" {
  for_each = aws_s3_bucket.data_lake

  bucket        = each.value.id
  target_bucket = aws_s3_bucket.access_logs.id
  target_prefix = "${each.key}/"
}

