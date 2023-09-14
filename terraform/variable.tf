variable "region" {
  description = "aws region"
  default     = "us-east-1"
}

variable "account_id" {
  default = 801772755864
}

variable "environment" {
  default = "dev"
}

variable "prefix" {
  description = "objects prefix"
  default     = "mastercard-datalake-dbm"
}

# Prefix configuration and project common tags
locals {
  glue_bucket = "${var.prefix}-scripts-${var.environment}-${var.account_id}"
  prefix      = var.prefix
  common_tags = {
    Environment = "dev"
    Project     = "dataflow-mc"
  }
}

variable "bucket_names" {
  description = "s3 bucket names"
  type        = list(string)
  default = [
    "landing-zone",
    "raw-zone",
    "trusted-zone",
    "refined-zone",
    "aws-glue-scripts",
    "scripts"
  ]
}

variable "glue_job_role_arn" {
  description = "The ARN of the IAM role associated with this job."
  default     = null
}