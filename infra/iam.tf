# The identity Glue jobs run as. It can touch this project's bucket, this
# project's catalog databases and Glue's own log groups, and nothing else.
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
    # Only Glue jobs in this account may use the role.
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "glue_job" {
  name               = "payments-lakehouse-glue-job"
  description        = "Runs the payments-lakehouse Spark jobs on AWS Glue."
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

locals {
  region_account  = "${var.region}:${local.account_id}"
  glue_db_names   = [for db in aws_glue_catalog_database.lakehouse : db.name]
  glue_db_arns    = [for n in local.glue_db_names : "arn:aws:glue:${local.region_account}:database/${n}"]
  glue_table_arns = [for n in local.glue_db_names : "arn:aws:glue:${local.region_account}:table/${n}/*"]
}

data "aws_iam_policy_document" "glue_job" {
  statement {
    sid       = "ListLakehouseBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lakehouse.arn]
  }

  statement {
    sid       = "ReadWriteLakehouseObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lakehouse.arn}/*"]
  }

  # What Iceberg's Glue catalog needs: read databases, create/read/update
  # tables. A commit is an UpdateTable call that only succeeds if nobody
  # else committed in between (optimistic locking).
  statement {
    sid = "IcebergCatalog"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
    ]
    resources = concat(
      ["arn:aws:glue:${local.region_account}:catalog"],
      local.glue_db_arns,
      local.glue_table_arns,
    )
  }

  statement {
    sid = "GlueJobLogs"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:aws:logs:${local.region_account}:log-group:/aws-glue/*"]
  }

  statement {
    sid       = "GlueJobMetrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"] # PutMetricData has no resource ARNs; limited by namespace below
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["Glue"]
    }
  }
}

resource "aws_iam_role_policy" "glue_job" {
  name   = "lakehouse-access"
  role   = aws_iam_role.glue_job.id
  policy = data.aws_iam_policy_document.glue_job.json
}
