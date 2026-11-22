variable "region" {
  description = "AWS region. New-style AWS accounts in India are limited to Asia Pacific (Sydney)."
  type        = string
  default     = "ap-southeast-2"
}

variable "alert_email" {
  description = "Where AWS Budgets sends cost alerts. Set it in terraform.tfvars (gitignored)."
  type        = string

  validation {
    condition     = can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.alert_email))
    error_message = "alert_email must be an email address."
  }
}

variable "monthly_budget_usd" {
  description = "Monthly cost budget in USD. Alerts fire at 20% and 100% of it."
  type        = number
  default     = 5
}

variable "athena_scan_limit_bytes" {
  description = "Athena cancels any query that would scan more than this (protects against an accidental full scan)."
  type        = number
  default     = 1073741824 # 1 GiB, about $0.005 at $5 per TB
}
