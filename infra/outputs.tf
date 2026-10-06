output "lake_bucket" {
  value       = aws_s3_bucket.lake.bucket
  description = "Iceberg lake bucket (also TICKSTREAM_S3_BUCKET)."
}

output "glue_database" {
  value       = aws_glue_catalog_database.lake.name
  description = "Glue database for bronze/silver/gold/ops tables."
}

output "athena_workgroup" {
  value       = aws_athena_workgroup.pipeline.name
  description = "Workgroup for dbt, scheduler maintenance, and Superset."
}

output "pipeline_iam_user" {
  value       = aws_iam_user.pipeline.name
  description = "Least-privilege user for all pipeline AWS access."
}

output "budget_name" {
  value       = aws_budgets_budget.monthly.name
  description = "Monthly cost budget guardrail."
}
