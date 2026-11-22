#!/usr/bin/env bash
# Run one SQL statement in Athena (workgroup payments-lakehouse) and print the result.
#
#   ./tools/athena_query.sh "SELECT count(*) FROM bronze.payments"
#
# Prints the rows tab-separated, then how much data the query scanned
# (Athena bills $5 per TB scanned; the workgroup cancels queries over 1 GiB).
set -euo pipefail

sql=${1:?usage: athena_query.sh "<SQL>"}
workgroup=payments-lakehouse

qid=$(aws athena start-query-execution --work-group "$workgroup" \
        --query-execution-context Catalog=AwsDataCatalog \
        --query-string "$sql" --query QueryExecutionId --output text)

while :; do
    state=$(aws athena get-query-execution --query-execution-id "$qid" \
              --query QueryExecution.Status.State --output text)
    case $state in SUCCEEDED|FAILED|CANCELLED) break ;; esac
    sleep 1
done

if [ "$state" != SUCCEEDED ]; then
    aws athena get-query-execution --query-execution-id "$qid" \
        --query QueryExecution.Status.StateChangeReason --output text >&2
    exit 1
fi

aws athena get-query-results --query-execution-id "$qid" --output json |
    python3 -c '
import json, sys
for row in json.load(sys.stdin)["ResultSet"]["Rows"]:
    print("\t".join(c.get("VarCharValue", "NULL") for c in row["Data"]))'

aws athena get-query-execution --query-execution-id "$qid" \
    --query 'QueryExecution.Statistics.[DataScannedInBytes,EngineExecutionTimeInMillis]' --output text |
    awk '{printf "(scanned %.1f MB in %.1f s)\n", $1/1048576, $2/1000}'
