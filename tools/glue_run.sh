#!/usr/bin/env bash
# Start an AWS Glue job run, wait for it to finish, and print the result.
#
#   ./tools/glue_run.sh <job-name> [--arg value ...]
#   ./tools/glue_run.sh payments-lakehouse-load-bronze --batch-id 20261122T070725Z
#
# Exits non-zero if the run doesn't succeed, and prints the end of its error log.
set -euo pipefail

job=${1:?usage: glue_run.sh <job-name> [--arg value ...]}
shift

# Build the Arguments map from "--name value" pairs.
args_json="{}"
while [ $# -gt 0 ]; do
    name=$1 value=${2:?missing value for $1}
    args_json=$(python3 -c 'import json,sys; d=json.loads(sys.argv[1]); d[sys.argv[2]]=sys.argv[3]; print(json.dumps(d))' \
                "$args_json" "$name" "$value")
    shift 2
done

# The job allows one run at a time, and Glue keeps the slot of a just-finished
# run busy for a short while ("Concurrent runs exceeded"), so retry for a bit.
err_file=$(mktemp)
trap 'rm -f "$err_file"' EXIT
for attempt in 1 2 3 4 5 6 7 8; do
    if run_id=$(aws glue start-job-run --job-name "$job" --arguments "$args_json" \
                  --query JobRunId --output text 2>"$err_file"); then
        break
    fi
    if ! grep -q ConcurrentRunsExceeded "$err_file" || [ "$attempt" = 8 ]; then
        cat "$err_file" >&2
        exit 1
    fi
    echo "  previous run still releasing its slot; retrying in 15 s"
    sleep 15
done
echo "Started $job run $run_id"

state=""
while :; do
    state=$(aws glue get-job-run --job-name "$job" --run-id "$run_id" --query JobRun.JobRunState --output text)
    case $state in
        SUCCEEDED|FAILED|ERROR|TIMEOUT|STOPPED|EXPIRED) break ;;
    esac
    printf '  %s %s\n' "$(date +%H:%M:%S)" "$state"
    sleep 15
done

aws glue get-job-run --job-name "$job" --run-id "$run_id" \
    --query 'JobRun.{state:JobRunState,seconds:ExecutionTime,dpu_seconds:DPUSeconds,workers:NumberOfWorkers,worker:WorkerType,glue:GlueVersion,error:ErrorMessage}' \
    --output table

# The driver's stdout (our print statements) goes to /aws-glue/jobs/output,
# errors to /aws-glue/jobs/error, in a log stream named after the run id.
if [ "$state" = SUCCEEDED ]; then
    echo "Job output:"
    aws logs get-log-events --log-group-name /aws-glue/jobs/output --log-stream-name "$run_id" \
        --query 'events[].message' --output text 2>/dev/null | tr '\t' '\n' | grep -v '^\s*$' | tail -30 || true
else
    echo "Last error log lines:"
    aws logs get-log-events --log-group-name /aws-glue/jobs/error --log-stream-name "$run_id" \
        --query 'events[].message' --output text 2>/dev/null | tr '\t' '\n' | grep -iE "error|exception|caused by" | tail -20 || true
    exit 1
fi
