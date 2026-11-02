
# ─────────────────────────────────────────────────────────────────────────────
# Root-Level Outputs
# ─────────────────────────────────────────────────────────────────────────────

# ─── S3 ──────────────────────────────────────────────────────────────────────

output "s3_bucket_arns" {
  description = "ARNs of all data lake S3 buckets"
  value       = module.s3_data_lake.bucket_arns
}

output "s3_bucket_names" {
  description = "Names of all data lake S3 buckets"
  value       = module.s3_data_lake.bucket_names
}

output "landing_bucket_name" {
  description = "Landing zone bucket name (for ingestion scripts)"
  value       = module.s3_data_lake.bucket_names["landing"]
}

# ─── IAM ─────────────────────────────────────────────────────────────────────

output "databricks_role_arn" {
  description = "IAM role ARN for Databricks to assume"
  value       = module.iam.databricks_role_arn
}

output "powerbi_role_arn" {
  description = "IAM role ARN for Power BI read access"
  value       = module.iam.powerbi_role_arn
}

output "cicd_role_arn" {
  description = "IAM role ARN for CI/CD pipeline"
  value       = module.iam.cicd_role_arn
}

# ─── Networking ──────────────────────────────────────────────────────────────

output "vpc_id" {
  description = "VPC ID"
  value       = module.networking.vpc_id
}

output "private_subnet_ids" {
  description = "Private subnet IDs (for Databricks clusters)"
  value       = module.networking.private_subnet_ids
}

output "security_group_id" {
  description = "Security group ID for Databricks"
  value       = module.networking.databricks_security_group_id
}

# ─── Monitoring ──────────────────────────────────────────────────────────────

output "sns_alert_topic_arn" {
  description = "SNS topic ARN for alerts"
  value       = module.monitoring.sns_topic_arn
}

