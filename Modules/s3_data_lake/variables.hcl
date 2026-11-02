
# ─────────────────────────────────────────────────────────────────────────────
# S3 Data Lake Module — Variables
# ─────────────────────────────────────────────────────────────────────────────

variable "name_prefix" {
  description = "Naming prefix for all resources"
  type        = string
}

variable "bucket_names" {
  description = "Map of layer name to bucket name"
  type        = map(string)
}

variable "landing_expiration_days" {
  description = "Days before landing zone files expire"
  type        = number
  default     = 30
}

variable "bronze_transition_days" {
  description = "Days before Bronze data moves to IA storage"
  type        = number
  default     = 90
}

variable "enable_glacier_archival" {
  description = "Enable Glacier archival for Gold layer"
  type        = bool
  default     = false
}

