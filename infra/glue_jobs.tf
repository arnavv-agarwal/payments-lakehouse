# Spark jobs on AWS Glue 6.0 (Spark 4.1.1, Iceberg 1.11.0, Python 3.13).
#
# The job scripts are the same files as in Phase 1 (spark/jobs/*.py). Terraform
# uploads them to s3://<bucket>/code/jobs/ and re-uploads when they change.
# The Spark settings below mirror spark/conf/spark-defaults.conf, except the
# catalog: `lakehouse` is the Glue Data Catalog here instead of the JDBC one,
# so the jobs' table names (lakehouse.bronze.payments, ...) stay the same.
# What else differs between local and Glue is handled in spark/jobs/lakehouse_env.py.
locals {
  bucket    = aws_s3_bucket.lakehouse.bucket
  warehouse = "s3://${local.bucket}/warehouse"
  code      = "s3://${local.bucket}/code/jobs"

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
    "--TempDir"          = "s3://${local.bucket}/tmp/"
    "--job-language"     = "python"
  }

  # job => script it runs, description, default arguments, helper modules it imports
  glue_jobs = {
    load_bronze = {
      script      = "load_bronze"
      description = "Append one landing batch (exported from the local bronze tables) to bronze on AWS."
      arguments   = { "--landing" = "s3://${local.bucket}/landing" }
      modules     = ["lakehouse_env"]
    }
    silver_cdc = {
      script      = "silver_cdc"
      description = "Apply new bronze change events to silver (current state per key), then stop."
      arguments = {
        "--available-now"   = "true"
        "--checkpoint-root" = "s3://${local.bucket}/checkpoints/silver"
        "--pii-key-param"   = local.pii_key_parameter
      }
      modules = ["lakehouse_env"]
    }
    gold_star = {
      script      = "gold_star"
      description = "Build the gold star schema (incremental)."
      arguments   = {}
      modules     = ["lakehouse_env"]
    }
    # Same script in check mode, as its own job: it runs right after gold_star,
    # and a job allows one run at a time (Glue keeps a finished run's slot busy
    # for a few seconds, so sharing gold_star's job made the check fail to start).
    gold_check = {
      script      = "gold_star"
      description = "Compare gold's incrementally maintained fact_payments with a full recomputation."
      arguments   = { "--check" = "true" }
      modules     = ["lakehouse_env"]
    }
    dq_checks = {
      script      = "dq_checks"
      description = "Data-quality checks on silver and gold; fails if any error-level check fails."
      arguments = {
        "--skip-source-checks" = "true" # no Postgres on AWS
        "--settle-minutes"     = "0"    # batch: silver has finished before the checks run
      }
      modules = ["lakehouse_env", "silver_cdc"]
    }
    maintenance = {
      script      = "maintenance"
      description = "Iceberg housekeeping for one layer: run with --layer bronze|silver|gold."
      arguments   = {}
      modules     = ["lakehouse_env"]
    }
  }

  # Every Python file Glue needs: the job scripts and the modules they import.
  python_files = toset(flatten([for j in local.glue_jobs : concat([j.script], j.modules)]))
}

resource "aws_s3_object" "job_code" {
  for_each = local.python_files

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
  number_of_workers = 2      # the minimum; our data is tens of MB
  execution_class   = "STANDARD"
  timeout           = 15 # minutes: a stuck run can't bill for long
  max_retries       = 0  # retries are the orchestrator's decision, not Glue's

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "${local.code}/${each.value.script}.py"
  }

  default_arguments = merge(local.common_arguments, each.value.arguments, {
    "--extra-py-files" = join(",", [for m in each.value.modules : "${local.code}/${m}.py"])
  })

  execution_property {
    max_concurrent_runs = 1 # one writer at a time, as locally (max_active_runs=1)
  }

  # Upload the code before a job can point at it.
  depends_on = [aws_s3_object.job_code]
}

# Glue writes driver output and errors to these log groups. Creating them here
# (instead of letting Glue create them on first run) sets a retention period
# and lets `terraform destroy` remove them.
resource "aws_cloudwatch_log_group" "glue" {
  for_each = toset(["/aws-glue/jobs/output", "/aws-glue/jobs/error", "/aws-glue/jobs/logs-v2"])

  name              = each.key
  retention_in_days = 14
}

# The script upload was called job_script in step 9; same object, new name.
moved {
  from = aws_s3_object.job_script["load_bronze"]
  to   = aws_s3_object.job_code["load_bronze"]
}
