#!/usr/bin/env bash
# Delete everything Phase 2 created on AWS. Irreversible: the data in S3 goes too.
#
#   ./tools/teardown_aws.sh --plan   # only list what would be deleted
#   ./tools/teardown_aws.sh          # delete (asks you to type the bucket name)
#
# Removes, in order:
#   1. the email-hash key in Parameter Store (created by put_pii_key.sh, not Terraform)
#   2. everything Terraform created: the S3 bucket and all its files, Glue jobs,
#      databases and their tables, both IAM identities (and Airflow's key), the
#      Athena workgroup, the log groups and the budget
#   3. the now-dead Airflow key values in .env
# then checks that nothing is left. The AWS account itself stays; with nothing
# in it, it costs nothing.
set -euo pipefail
cd "$(dirname "$0")/.."

param=/payments-lakehouse/pii-hash-key

if [ "${1:-}" = "--plan" ]; then
    terraform -chdir=infra plan -destroy -input=false -no-color | grep -E '^  # |^Plan:' || true
    aws ssm describe-parameters --parameter-filters "Key=Name,Values=$param" \
        --query 'Parameters[].Name' --output text | sed 's/^./  # outside Terraform: SSM parameter &/'
    exit 0
fi

bucket=$(terraform -chdir=infra output -raw bucket)
user=$(terraform -chdir=infra output -raw airflow_user)
echo "This deletes ALL Phase 2 resources on AWS, including every file in s3://$bucket."
read -r -p "Type the bucket name to confirm: " answer
[ "$answer" = "$bucket" ] || { echo "Not confirmed; nothing deleted."; exit 1; }

if aws ssm delete-parameter --name "$param" 2>/dev/null; then
    echo "Deleted SSM parameter $param"
else
    echo "SSM parameter $param was already gone"
fi

terraform -chdir=infra destroy -auto-approve -input=false

# The key was deleted with the IAM user; blank its values so nothing tries it.
python3 - <<'EOF'
dead = {"AWS_AIRFLOW_ACCESS_KEY_ID", "AWS_AIRFLOW_SECRET_ACCESS_KEY", "AWS_LAKEHOUSE_BUCKET"}
with open(".env") as f:
    lines = f.read().splitlines()
lines = [f"{l.split('=', 1)[0]}=" if l.split("=", 1)[0] in dead else l for l in lines]
with open(".env", "w") as f:
    f.write("\n".join(lines) + "\n")
print("Cleared the Airflow AWS key in .env")
EOF

echo "Checking that nothing is left:"
left=0
aws s3api head-bucket --bucket "$bucket" 2>/dev/null && { echo "  bucket $bucket still exists"; left=1; }
jobs=$(aws glue get-jobs --query "Jobs[?starts_with(Name, 'payments-lakehouse')].Name" --output text)
[ -n "$jobs" ] && { echo "  Glue jobs: $jobs"; left=1; }
dbs=$(aws glue get-databases --query 'DatabaseList[].Name' --output text)
[ -n "$dbs" ] && { echo "  Glue databases: $dbs"; left=1; }
aws iam get-user --user-name "$user" >/dev/null 2>&1 && { echo "  IAM user $user"; left=1; }
aws iam get-role --role-name payments-lakehouse-glue-job >/dev/null 2>&1 && { echo "  IAM role payments-lakehouse-glue-job"; left=1; }
[ "$left" = 0 ] && echo "  nothing: teardown complete"
exit "$left"
