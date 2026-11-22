#!/usr/bin/env bash
# Create (or rotate) the access key of the IAM user that Airflow uses, and
# write it into .env together with the bucket name. Nothing secret is printed.
#
#   ./tools/create_airflow_key.sh
#   docker compose up -d airflow      # restart Airflow to pick up the new key
#
# Rotation: existing keys of the user are deleted first, so only one key is
# ever valid. The key lives only in .env (gitignored), not in Terraform state.
set -euo pipefail
cd "$(dirname "$0")/.."

user=$(terraform -chdir=infra output -raw airflow_user)
bucket=$(terraform -chdir=infra output -raw bucket)
region=$(terraform -chdir=infra output -raw region)

for old in $(aws iam list-access-keys --user-name "$user" --query 'AccessKeyMetadata[].AccessKeyId' --output text); do
    aws iam delete-access-key --user-name "$user" --access-key-id "$old"
    echo "Deleted old key ...${old: -4}"
done

aws iam create-access-key --user-name "$user" --output json |
python3 -c '
import json, re, sys
key = json.load(sys.stdin)["AccessKey"]
values = {
    "AWS_AIRFLOW_ACCESS_KEY_ID": key["AccessKeyId"],
    "AWS_AIRFLOW_SECRET_ACCESS_KEY": key["SecretAccessKey"],
    "AWS_LAKEHOUSE_BUCKET": sys.argv[1],
    "AWS_LAKEHOUSE_REGION": sys.argv[2],
}
comment = "# Phase 2 (step 11): Airflow -> AWS, written by tools/create_airflow_key.sh"
with open(".env") as f:
    lines = [l for l in f.read().splitlines()
             if l.split("=", 1)[0] not in values and l != comment]
lines += [comment] + [f"{k}={v}" for k, v in values.items()]
with open(".env", "w") as f:
    f.write("\n".join(lines) + "\n")
key_id, user = key["AccessKeyId"], key["UserName"]
print(f"Created key ...{key_id[-4:]} for {user}; written to .env")
' "$bucket" "$region"
