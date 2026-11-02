
# ─────────────────────────────────────────────────────────────────────────────
# Monitoring Module — Variables
# ─────────────────────────────────────────────────────────────────────────────

variable "name_prefix" {
  description = "Naming prefix for all resources"
  type        = string
}

variable "alert_email" {
  description = "Email address for alerts"
  type        = string
}

variable "monthly_budget_limit" {
  description = "Monthly budget limit in USD"
  type        = number
}

variable "bucket_names" {
  description = "Map of layer name to bucket name"
  type        = map(string)
}

