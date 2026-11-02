
# ─────────────────────────────────────────────────────────────────────────────
# Monitoring Module — Outputs
# ─────────────────────────────────────────────────────────────────────────────

output "sns_topic_arn" {
  description = "SNS alert topic ARN"
  value       = aws_sns_topic.alerts.arn
}

output "budget_name" {
  description = "AWS Budget name"
  value       = aws_budgets_budget.monthly.name
}

