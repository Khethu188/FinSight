
variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "af-south-1"
}

variable "state_bucket_name" {
  description = "S3 bucket name for Terraform state"
  type        = string
  default     = "fdp-terraform-state"
}

variable "lock_table_name" {
  description = "DynamoDB table name for state locking"
  type        = string
  default     = "fdp-terraform-locks"
}

