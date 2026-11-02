
# ─────────────────────────────────────────────────────────────────────────────
# S3 Lifecycle Policies — Cost Optimization
# ─────────────────────────────────────────────────────────────────────────────

# Landing zone: Delete processed files after N days
resource "aws_s3_bucket_lifecycle_configuration" "landing" {
  bucket = aws_s3_bucket.data_lake["landing"].id

  rule {
    id     = "expire-processed-landing-files"
    status = "Enabled"

    filter {
      prefix = "landing/"
    }

    expiration {
      days = var.landing_expiration_days
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"

    abort_incomplete_multipart_upload {
      days_after_initiation = 3
    }
  }
}

# Bronze: Transition to Infrequent Access after N days
resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.data_lake["bronze"].id

  rule {
    id     = "transition-bronze-to-ia"
    status = "Enabled"

    filter {
      prefix = ""
    }

    transition {
      days          = var.bronze_transition_days
      storage_class = "STANDARD_IA"
    }
  }

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"

    abort_incomplete_multipart_upload {
      days_after_initiation = 3
    }
  }
}

# Gold: Optional Glacier archival for historical data
resource "aws_s3_bucket_lifecycle_configuration" "gold" {
  count  = var.enable_glacier_archival ? 1 : 0
  bucket = aws_s3_bucket.data_lake["gold"].id

  rule {
    id     = "archive-old-gold-to-glacier"
    status = "Enabled"

    filter {
      prefix = ""
    }

    transition {
      days          = 365
      storage_class = "GLACIER"
    }
  }
}

# Quarantine: Auto-expire quarantined records after 60 days
resource "aws_s3_bucket_lifecycle_configuration" "quarantine" {
  bucket = aws_s3_bucket.data_lake["quarantine"].id

  rule {
    id     = "expire-quarantine-records"
    status = "Enabled"

    filter {
      prefix = ""
    }

    expiration {
      days = 60
    }
  }
}

# Manifests: Keep for 180 days (audit trail)
resource "aws_s3_bucket_lifecycle_configuration" "manifests" {
  bucket = aws_s3_bucket.data_lake["manifests"].id

  rule {
    id     = "expire-old-manifests"
    status = "Enabled"

    filter {
      prefix = ""
    }

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    expiration {
      days = 180
    }
  }
}

