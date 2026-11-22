# The identity the local Airflow uses to drive the AWS side of the pipeline.
#
# Airflow runs on the laptop, outside AWS, so it can't simply "be" an AWS role
# the way a service inside AWS can. It gets an IAM user with an access key
# (created by tools/create_airflow_key.sh straight into .env, so the secret
# never lands in Terraform state). The user can do exactly what the DAG needs:
#   - upload landing batches
#   - start, watch and stop this project's Glue jobs, and read their logs
#   - query the gold tables through the Athena workgroup
# It can't read bronze or silver (raw or personal data), change any table,
# touch IAM, or start any other job.
#
# A managed Airflow inside AWS (MWAA) would use an execution role instead, with
# no long-lived key at all.
resource "aws_iam_user" "airflow" {
  name = "payments-lakehouse-airflow"

  # Lets `terraform destroy` delete the user even though it has an access key.
  force_destroy = true
}

data "aws_iam_policy_document" "airflow" {
  statement {
    sid       = "UploadLandingBatches"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.lakehouse.arn}/landing/*"]
  }

  statement {
    sid = "RunProjectGlueJobs"
    actions = [
      "glue:GetJob",
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun",
    ]
    resources = ["arn:aws:glue:${local.region_account}:job/payments-lakehouse-*"]
  }

  statement {
    sid = "ReadGlueJobLogs"
    actions = [
      "logs:DescribeLogStreams",
      "logs:GetLogEvents",
      "logs:FilterLogEvents",
    ]
    resources = [
      "arn:aws:logs:${local.region_account}:log-group:/aws-glue/jobs/*",
      "arn:aws:logs:${local.region_account}:log-group:/aws-glue/jobs/*:log-stream:*",
    ]
  }

  statement {
    sid = "QueryWithAthena"
    actions = [
      "athena:GetWorkGroup",
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:StopQueryExecution",
    ]
    resources = [aws_athena_workgroup.lakehouse.arn]
  }

  # Athena reads the catalog and the files with the caller's permissions:
  # gold only.
  statement {
    sid = "ReadGoldCatalog"
    actions = [
      "glue:GetCatalog",
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
    ]
    resources = [
      "arn:aws:glue:${local.region_account}:catalog",
      "arn:aws:glue:${local.region_account}:database/gold",
      "arn:aws:glue:${local.region_account}:table/gold/*",
    ]
  }

  statement {
    sid       = "ListBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lakehouse.arn]
  }

  statement {
    sid       = "ReadGoldFiles"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lakehouse.arn}/warehouse/gold/*"]
  }

  statement {
    sid       = "AthenaResults"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.lakehouse.arn}/athena-results/*"]
  }
}

resource "aws_iam_user_policy" "airflow" {
  name   = "drive-pipeline"
  user   = aws_iam_user.airflow.name
  policy = data.aws_iam_policy_document.airflow.json
}

output "airflow_user" {
  value = aws_iam_user.airflow.name
}
