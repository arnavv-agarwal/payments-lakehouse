data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  # Bucket names are global across all AWS customers; the account id makes it unique.
  bucket_name = "payments-lakehouse-${local.account_id}"
}

# One bucket for the whole lakehouse:
#   warehouse/       Iceberg tables (data + metadata files)
#   landing/         files uploaded from the local stack before loading
#   code/            Spark job scripts that Glue runs
#   tmp/             Glue's scratch space
#   athena-results/  Athena query results
resource "aws_s3_bucket" "lakehouse" {
  bucket = local.bucket_name

  # Lets `terraform destroy` delete the bucket even when it still holds files.
  # Right for a demo with synthetic data; never for real data.
  force_destroy = true
}

# Nothing in this bucket is ever public.
resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Access is decided by IAM policies only; per-object ACLs are switched off.
resource "aws_s3_bucket_ownership_controls" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Encrypt every object at rest with S3-managed keys (free).
resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Refuse any request that isn't over HTTPS.
data "aws_iam_policy_document" "tls_only" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.lakehouse.arn,
      "${aws_s3_bucket.lakehouse.arn}/*",
    ]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  policy = data.aws_iam_policy_document.tls_only.json

  depends_on = [aws_s3_bucket_public_access_block.lakehouse]
}

# Housekeeping rules so short-lived files don't pile up.
# (No versioning: Iceberg already keeps table versions as snapshots.)
resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 7
    }
  }

  rule {
    id     = "expire-glue-tmp"
    status = "Enabled"
    filter {
      prefix = "tmp/"
    }
    expiration {
      days = 7
    }
  }

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}
