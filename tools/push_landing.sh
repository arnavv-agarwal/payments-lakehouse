#!/usr/bin/env bash
# Upload local landing batches (written by spark/jobs/export_bronze.py) to S3.
#
#   ./tools/push_landing.sh
#
# Uses your AWS CLI login (`aws login`); no AWS credentials go into containers.
# `aws s3 sync` only uploads files that aren't in S3 yet, so re-running is safe.
set -euo pipefail
cd "$(dirname "$0")/.."

bucket=$(terraform -chdir=infra output -raw bucket)

# Spark's local-filesystem bookkeeping files (*.crc checksums, _SUCCESS markers)
# aren't data; the batch manifest is the completeness record.
aws s3 sync landing/bronze "s3://${bucket}/landing/bronze" \
    --exclude "*.crc" --exclude "*/_SUCCESS" --only-show-errors

echo "Batches in s3://${bucket}/landing/bronze/_manifests/:"
aws s3 ls "s3://${bucket}/landing/bronze/_manifests/" | awk '{print "  " $4}'
