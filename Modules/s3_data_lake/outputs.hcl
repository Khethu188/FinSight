
# ─────────────────────────────────────────────────────────────────────────────
# S3 Data Lake Module — Outputs
# ─────────────────────────────────────────────────────────────────────────────

output "bucket_arns" {
  description = "Map of layer name to bucket ARN"
  value = {
    for k, v in aws_s3_bucket.data_lake : k => v.arn
  }
}

output "bucket_names" {
  description = "Map of layer name to bucket name"
  value = {
    for k, v in aws_s3_bucket.data_lake : k => v.id
  }
}

output "bucket_ids" {
  description = "Map of layer name to bucket ID"
  value = {
    for k, v in aws_s3_bucket.data_lake : k => v.id
  }
}

output "access_logs_bucket" {
  description = "Access logs bucket name"
  value       = aws_s3_bucket.access_logs.id
}

