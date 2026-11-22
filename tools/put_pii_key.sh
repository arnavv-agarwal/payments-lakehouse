#!/usr/bin/env bash
# Copy PII_HASH_KEY from .env into AWS Systems Manager Parameter Store as an
# encrypted SecureString, for the silver job on Glue.
#
#   ./tools/put_pii_key.sh
#
# The same key as locally, so email_hash values are identical in both places.
# The value never appears on the command line or in Terraform state: it goes
# through a private temporary file. `terraform destroy` doesn't know about this
# parameter; tools/teardown_aws.sh deletes it.
set -euo pipefail
cd "$(dirname "$0")/.."

name=/payments-lakehouse/pii-hash-key
value=$(grep -E '^PII_HASH_KEY=' .env | head -1 | cut -d= -f2-)
[ -n "$value" ] || { echo "PII_HASH_KEY not found in .env" >&2; exit 1; }

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
chmod 600 "$tmp"
printf '%s' "$value" > "$tmp"   # no trailing newline: it would become part of the key

aws ssm put-parameter --name "$name" --type SecureString --overwrite \
    --description "HMAC key for hashing customer emails in silver (payments-lakehouse)" \
    --value "file://$tmp" --query Version --output text |
    xargs printf 'Stored %s (version %s), encrypted with the AWS-managed key aws/ssm\n' "$name"
