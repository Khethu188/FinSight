
# ─────────────────────────────────────────────────────────────────────────────
# IAM Module — Outputs
# ─────────────────────────────────────────────────────────────────────────────

output "databricks_role_arn" {
  description = "Databricks cross-account role ARN"
  value       = aws_iam_role.databricks.arn
}

output "databricks_role_name" {
  description = "Databricks role name"
  value       = aws_iam_role.databricks.name
}

output "powerbi_role_arn" {
  description = "Power BI read-only role ARN"
  value       = aws_iam_role.powerbi.arn
}

output "cicd_role_arn" {
  description = "CI/CD pipeline role ARN"
  value       = aws_iam_role.cicd.arn
}

output "ingestion_role_arn" {
  description = "Ingestion role ARN (landing zone write)"
  value       = aws_iam_role.ingestion.arn
}

