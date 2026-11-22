# Spark jobs on AWS Glue 6.0 (Spark 4.1.1, Iceberg 1.11.0, Python 3.13).
#
# The job scripts are the same files as in Phase 1 (spark/jobs/*.py). Terraform
# uploads them to s3://<bucket>/code/jobs/ and re-uploads when they change.
# The Spark settings below mirror spark/conf/spark-defaults.conf, except the
# catalog: `lakehouse` is the Glue Data Catalog here instead of the JDBC one,
# so the jobs' table names (lakehouse.bronze.payments, ...) stay the same.
locals {
  warehouse = "s3://${aws_s3_bucket.lakehouse.bucket}/warehouse"

  # Glue takes extra Spark settings as one "--conf" argument whose value
  # chains further "--conf" pairs (documented Glue convention).
  spark_conf = join(" --conf ", [
    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lakehouse.type=glue",
    "spark.sql.catalog.lakehouse.warehouse=${local.warehouse}",
    "spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.defaultCatalog=lakehouse",
    "spark.sql.session.timeZone=UTC",
    "spark.sql.shuffle.partitions=8",
  ])

  common_arguments = {
    "--datalake-formats" = "iceberg"
    "--conf"             = local.spark_conf
    "--TempDir"          = "s3://${aws_s3_bucket.lakehouse.bucket}/tmp/"
    "--job-language"     = "python"
  }

  glue_jobs = {
    load_bronze = {
      description = "Append one landing batch (exported from the local bronze tables) to bronze on AWS."
      arguments = {
        "--landing" = "s3://${aws_s3_bucket.lakehouse.bucket}/landing"
      }
    }
  }
}

resource "aws_s3_object" "job_script" {
  for_each = local.glue_jobs

  bucket      = aws_s3_bucket.lakehouse.id
  key         = "code/jobs/${each.key}.py"
  source      = "${path.module}/../spark/jobs/${each.key}.py"
  source_hash = filemd5("${path.module}/../spark/jobs/${each.key}.py")
}

resource "aws_glue_job" "job" {
  for_each = local.glue_jobs

  name        = "payments-lakehouse-${replace(each.key, "_", "-")}"
  description = each.value.description
  role_arn    = aws_iam_role.glue_job.arn

  glue_version      = "6.0"
  worker_type       = "G.1X" # 4 vCPU, 16 GB per worker; billed as 1 DPU each
  number_of_workers = 2      # the minimum; our batches are tens of MB
  execution_class   = "STANDARD"
  timeout           = 15 # minutes: a stuck run can't bill for long
  max_retries       = 0  # retries are Airflow's decision, not Glue's

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lakehouse.bucket}/${aws_s3_object.job_script[each.key].key}"
  }

  default_arguments = merge(local.common_arguments, each.value.arguments)

  execution_property {
    max_concurrent_runs = 1 # same rule as Airflow's max_active_runs=1 locally
  }
}

# Glue writes driver output and errors to these log groups. Creating them here
# (instead of letting Glue create them on first run) sets a retention period
# and lets `terraform destroy` remove them.
resource "aws_cloudwatch_log_group" "glue" {
  for_each = toset(["/aws-glue/jobs/output", "/aws-glue/jobs/error", "/aws-glue/jobs/logs-v2"])

  name              = each.key
  retention_in_days = 14
}
