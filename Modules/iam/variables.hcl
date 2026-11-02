
# ─────────────────────────────────────────────────────────────────────────────
# IAM Module — Variables
# ─────────────────────────────────────────────────────────────────────────────

variable "name_prefix" {
  description = "Naming prefix for all resources"
  type        = string
}

variable "bucket_arns" {
  description = "Map of all bucket ARNs"
  type        = map(string)
}

variable "landing_bucket" {
  description = "Landing bucket ARN"
  type        = string
}

variable "bronze_bucket" {
  description = "Bronze bucket ARN"
  type        = string
}

variable "silver_bucket" {
  description = "Silver bucket ARN"
  type        = string
}

variable "gold_bucket" {
  description = "Gold bucket ARN"
  type        = string
}

variable "quarantine_bucket" {
  description = "Quarantine bucket ARN"
  type        = string
}

