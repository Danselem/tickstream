# Least-privilege pipeline user for producer/consumer/scheduler/dbt/Superset.
# No access keys are created here on purpose: access keys are secrets, and tofu
# state is not the place for them. Create the key once via console/CLI, store it
# in .env (gitignored), and rotate manually. See docs/runbook.md.

resource "aws_iam_user" "pipeline" {
  name = "${var.project}-pipeline"
}

resource "aws_iam_user_policy" "pipeline" {
  name = "${var.project}-lake-access"
  user = aws_iam_user.pipeline.name

  policy = data.aws_iam_policy_document.pipeline.json
}

data "aws_iam_policy_document" "pipeline" {
  # Iceberg data files + Athena results.
  statement {
    sid       = "LakeObjects"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
  }

  statement {
    sid = "LakeObjectRW"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }

  # Glue catalog: read the DB, manage our tables/partitions (dbt full refreshes
  # drop and recreate tables; PyIceberg commits touch table metadata).
  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchGetPartition",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
      "glue:UpdatePartition",
    ]
    resources = [
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      aws_glue_catalog_database.lake.arn,
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.lake.name}/*",
    ]
  }

  # Athena: run queries only through our workgroup.
  statement {
    sid = "AthenaQueries"
    actions = [
      "athena:StartQueryExecution",
      "athena:StopQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:BatchGetQueryExecution",
      "athena:GetWorkGroup",
    ]
    resources = [aws_athena_workgroup.pipeline.arn]
  }
}
