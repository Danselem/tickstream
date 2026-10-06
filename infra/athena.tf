# Dedicated workgroup for dbt + scheduler + Superset queries.
# AUTO tracks the latest engine (v3), required for Iceberg MERGE, OPTIMIZE, VACUUM.

resource "aws_athena_workgroup" "pipeline" {
  name = "${var.project}-pipeline"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    engine_version {
      selected_engine_version = "AUTO"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
    }
  }
}
