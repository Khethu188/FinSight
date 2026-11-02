
# ─────────────────────────────────────────────────────────────────────────────
# IAM Module — Least-Privilege Roles for Platform Components
# ─────────────────────────────────────────────────────────────────────────────
# Roles:
#   1. Databricks — Read/Write to all layers (Bronze→Gold pipeline)
#   2. Power BI   — Read-only on Gold layer (dashboards)
#   3. CI/CD      — Deploy Terraform + Databricks jobs
#   4. Ingestion  — Write to Landing zone only
# ─────────────────────────────────────────────────────────────────────────────

data "aws_caller_identity" "current" {}

# ─── 1. Databricks Role ─────────────────────────────────────────────────────

resource "aws_iam_role" "databricks" {
  name = "${var.name_prefix}-databricks-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::414351767826:root"  # Databricks AWS account
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.name_prefix
          }
        }
      }
    ]
  })

  tags = {
    Component = "databricks"
  }
}

resource "aws_iam_role_policy" "databricks_s3" {
  name = "${var.name_prefix}-databricks-s3-policy"
  role = aws_iam_role.databricks.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListBuckets"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = values(var.bucket_arns)
      },
      {
        Sid    = "ReadAllLayers"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
        ]
        Resource = [for arn in values(var.bucket_arns) : "${arn}/*"]
      },
      {
        Sid    = "WriteBronzeSilverGoldQuarantine"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:DeleteObject",
        ]
        Resource = [
          "${var.bronze_bucket}/*",
          "${var.silver_bucket}/*",
          "${var.gold_bucket}/*",
          "${var.quarantine_bucket}/*",
        ]
      },
      {
        Sid    = "DeltaLakeOperations"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          "${var.bronze_bucket}/_delta_log/*",
          "${var.silver_bucket}/_delta_log/*",
          "${var.gold_bucket}/_delta_log/*",
        ]
      }
    ]
  })
}

# ─── 2. Power BI Role (Read-Only on Gold) ───────────────────────────────────

resource "aws_iam_role" "powerbi" {
  name = "${var.name_prefix}-powerbi-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = data.aws_caller_identity.current.arn
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Component = "powerbi"
  }
}

resource "aws_iam_role_policy" "powerbi_read" {
  name = "${var.name_prefix}-powerbi-read-policy"
  role = aws_iam_role.powerbi.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListGoldBucket"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = var.gold_bucket
      },
      {
        Sid    = "ReadGoldData"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
        ]
        Resource = "${var.gold_bucket}/*"
      }
    ]
  })
}

# ─── 3. CI/CD Pipeline Role ─────────────────────────────────────────────────

resource "aws_iam_role" "cicd" {
  name = "${var.name_prefix}-cicd-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:oidc-provider/token.actions.githubusercontent.com"
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringLike = {
            "token.actions.githubusercontent.com:sub" = "repo:Khethu188/financial-data-platform:*"
          }
        }
      }
    ]
  })

  tags = {
    Component = "cicd"
  }
}

resource "aws_iam_role_policy" "cicd_deploy" {
  name = "${var.name_prefix}-cicd-deploy-policy"
  role = aws_iam_role.cicd.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "TerraformStateAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
        ]
        Resource = [
          "arn:aws:s3:::fdp-terraform-state",
          "arn:aws:s3:::fdp-terraform-state/*",
        ]
      },
      {
        Sid    = "TerraformLocking"
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:DeleteItem",
        ]
        Resource = "arn:aws:dynamodb:*:*:table/fdp-terraform-locks"
      },
      {
        Sid    = "ManageS3Buckets"
        Effect = "Allow"
        Action = [
          "s3:CreateBucket",
          "s3:DeleteBucket",
          "s3:PutBucketPolicy",
          "s3:PutBucketVersioning",
          "s3:PutEncryptionConfiguration",
          "s3:PutLifecycleConfiguration",
          "s3:PutBucketPublicAccessBlock",
          "s3:PutBucketLogging",
          "s3:PutBucketTagging",
          "s3:GetBucket*",
          "s3:ListBucket",
        ]
        Resource = "arn:aws:s3:::${var.name_prefix}-*"
      },
      {
        Sid    = "ManageIAM"
        Effect = "Allow"
        Action = [
          "iam:CreateRole",
          "iam:DeleteRole",
          "iam:PutRolePolicy",
          "iam:DeleteRolePolicy",
          "iam:GetRole",
          "iam:GetRolePolicy",
          "iam:ListRolePolicies",
          "iam:PassRole",
          "iam:TagRole",
          "iam:UntagRole",
        ]
        Resource = "arn:aws:iam::*:role/${var.name_prefix}-*"
      }
    ]
  })
}

# ─── 4. Ingestion Role (Landing Zone Write Only) ────────────────────────────

resource "aws_iam_role" "ingestion" {
  name = "${var.name_prefix}-ingestion-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = data.aws_caller_identity.current.arn
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Component = "ingestion"
  }
}

resource "aws_iam_role_policy" "ingestion_landing" {
  name = "${var.name_prefix}-ingestion-landing-policy"
  role = aws_iam_role.ingestion.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListLandingBucket"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = var.landing_bucket
      },
      {
        Sid    = "WriteLandingData"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectTagging",
        ]
        Resource = "${var.landing_bucket}/*"
      },
      {
        Sid      = "DenyReadOtherBuckets"
        Effect   = "Deny"
        Action   = "s3:GetObject"
        Resource = [
          "${var.bronze_bucket}/*",
          "${var.silver_bucket}/*",
          "${var.gold_bucket}/*",
        ]
      }
    ]
  })
}

