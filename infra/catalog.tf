# The Glue Data Catalog plays the role the local `iceberg-catalog` Postgres
# plays in Phase 1: it maps each table name to the table's current Iceberg
# metadata file. Each Glue database is one Iceberg namespace, with the same
# names as locally, so the jobs' SQL (`silver.payments`, ...) doesn't change.
locals {
  namespaces = {
    bronze = "Every CDC change event, as received (append only)."
    silver = "Current state per key, deduplicated, PII hashed."
    gold   = "Star schema for analysis: facts and dimensions."
    ops    = "Pipeline bookkeeping: data-quality results."
  }
}

resource "aws_glue_catalog_database" "lakehouse" {
  for_each = local.namespaces

  name         = each.key
  description  = each.value
  location_uri = "s3://${aws_s3_bucket.lakehouse.bucket}/warehouse/${each.key}"
}
