# Athena runs SQL directly on the Iceberg tables in S3, using the Glue catalog
# to find them. Billing is per byte scanned, so the workgroup cancels any
# query that would scan more than athena_scan_limit_bytes.
resource "aws_athena_workgroup" "lakehouse" {
  name          = "payments-lakehouse"
  description   = "Queries on the payments lakehouse (gold star schema)."
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_scan_limit_bytes

    engine_version {
      selected_engine_version = "AUTO"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lakehouse.bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
