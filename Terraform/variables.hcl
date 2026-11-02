
# ─────────────────────────────────────────────────────────────────────────────
# Root-Level Input Variables
# ─────────────────────────────────────────────────────────────────────────────

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "af-south-1"
}

variable "project_name" {
  description = "Project name used in resource naming and tagging"
  type        = string
  default     = "financial-data-platform"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "owner" {
  description = "Team or individual responsible for these resources"
  type        = string
  default     = "data-engineering"
}

# ─── Networking ──────────────────────────────────────────────────────────────

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "List of availability zones to use"
  type        = list(string)
  default     = ["af-south-1a", "af-south-1b"]
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets"
  type        = list(string)
  default     = ["10.0.101.0/24", "10.0.102.0/24"]
}

# ─── Cost Controls ───────────────────────────────────────────────────────────

variable "monthly_budget_limit" {
  description = "Monthly AWS budget limit in USD"
  type        = number
  default     = 50
}

variable "alert_email" {
  description = "Email address for budget and monitoring alerts"
  type        = string
}

# ─── S3 Lifecycle ────────────────────────────────────────────────────────────

variable "landing_expiration_days" {
  description = "Days before landing zone files are deleted (already processed)"
  type        = number
  default     = 30
}

variable "bronze_transition_days" {
  description = "Days before Bronze data transitions to Infrequent Access"
  type        = number
  default     = 90
}

variable "enable_glacier_archival" {
  description = "Enable Glacier archival for old Gold-layer data"
  type        = bool
  default     = false
}

