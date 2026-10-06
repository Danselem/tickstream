variable "project" {
  type        = string
  default     = "tickstream"
  description = "Name prefix for every resource."
}

variable "aws_region" {
  type        = string
  default     = "us-east-1"
  description = "AWS region for all resources (S3, Glue, Athena must share it)."
}

variable "alert_email" {
  type        = string
  sensitive   = true
  description = "Email for the AWS budget alert. Pass via TF_VAR_alert_email or -var; never commit."

  validation {
    condition     = can(regex("^[^@]+@[^@]+$", var.alert_email))
    error_message = "alert_email must be a valid email address."
  }
}

variable "monthly_budget_usd" {
  type        = string
  default     = "20"
  description = "Monthly cost budget in USD that triggers the alert."
}
