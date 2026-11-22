"""Orchestration for the payments lakehouse.

Airflow schedules and monitors; it doesn't compute. Locally, every task starts
a short-lived container from the lakehouse Spark image and waits for it. On
AWS (Phase 2), tasks start AWS Glue jobs and wait for them. The streaming jobs
(bronze, silver) run continuously as their own local services; Airflow handles
the batch work around them:

    lakehouse_gold             every 10 min     build gold -> verify it two ways
    lakehouse_maintenance      daily 02:30 IST  compact + clean up bronze -> silver -> gold
    lakehouse_aws              manual           ship new bronze rows to AWS, then run
                                                load -> silver -> gold -> checks on Glue,
                                                and a report from gold via Athena
    lakehouse_aws_maintenance  manual           Iceberg housekeeping on AWS, layer by layer
"""
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pendulum
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sdk import DAG
from docker.types import Mount

SPARK_IMAGE = "payments-lakehouse/spark:4.1.4-iceberg-1.12.0"
JOBS = "/opt/lakehouse/jobs"
# Passed as private_environment: unlike `environment`, these are not shown in
# the Airflow UI's rendered-template view or in logs.
SECRETS = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "ICEBERG_CATALOG_PASSWORD",
           "PII_HASH_KEY", "DEBEZIUM_PASSWORD"]


def spark_task(task_id, script, *args, py_files=None, landing=False, **kwargs):
    """A task that runs one Spark job in its own container.

    landing=True also mounts the project's landing/ folder at /landing (for
    export_bronze.py, which writes landing batches there).
    """
    host_dir = os.environ.get("LAKEHOUSE_HOST_DIR", "")
    command = ["/opt/spark/bin/spark-submit"]
    if py_files:
        command += ["--py-files", f"{JOBS}/{py_files}"]
    command += [f"{JOBS}/{script}", *args]
    mounts = [Mount(source=f"{host_dir}/spark/jobs", target=JOBS, type="bind", read_only=True)]
    if landing:
        mounts.append(Mount(source=f"{host_dir}/landing", target="/landing", type="bind"))
    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,
        command=command,
        docker_url=os.environ.get("DOCKER_HOST", "tcp://docker-proxy:2375"),
        network_mode=os.environ.get("LAKEHOUSE_NETWORK", "payments-lakehouse_default"),
        # Bind-mount the job code from the project folder (a path on the Docker
        # host, not inside the Airflow container), like the streaming services do.
        mounts=mounts,
        environment={"AWS_REGION": "us-east-1", "POSTGRES_DB": "payments"},
        private_environment={k: os.environ.get(k, "") for k in SECRETS},
        # The default mounts a temp dir from the Airflow container, a path the
        # Docker host doesn't have; the jobs don't need one.
        mount_tmp_dir=False,
        auto_remove="success",  # keep failed containers for inspection
        **kwargs,
    )


DEFAULTS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=8),
}

with DAG(
    dag_id="lakehouse_gold",
    description="Update the gold star schema from silver, then verify it",
    schedule=timedelta(minutes=10),
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,  # runs never overlap: gold's incremental state assumes one writer
    default_args=DEFAULTS,
    tags=["lakehouse", "gold"],
) as gold_dag:
    build = spark_task("build_gold", "gold_star.py")
    # Recompute fact_payments in full from the exact snapshots build_gold read,
    # and fail if the incremental result differs in any row.
    matches_full_rebuild = spark_task("check_incremental_matches_full_rebuild", "gold_star.py", "--check")
    data_quality = spark_task("data_quality_checks", "dq_checks.py", py_files="silver_cdc.py")
    build >> [matches_full_rebuild, data_quality]

with DAG(
    dag_id="lakehouse_maintenance",
    description="Compact files, clean up delete files, expire snapshots, remove orphan files",
    schedule="30 2 * * *",  # 02:30 India time, the quietest hour of the day
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULTS, "execution_timeout": timedelta(minutes=30)},
    tags=["lakehouse", "maintenance"],
) as maintenance_dag:
    previous = None
    for layer in ["bronze", "silver", "gold"]:  # one layer at a time, to limit load
        task = spark_task(f"maintain_{layer}", "maintenance.py", "--layer", layer)
        if previous:
            previous >> task
        previous = task


# ----------------------------------------------------------------------------- Phase 2: AWS

AWS_CONN = "aws_lakehouse"  # from AIRFLOW_CONN_AWS_LAKEHOUSE (docker-compose.yml)
LANDING = Path("/opt/lakehouse/landing/bronze")  # the project's landing/ folder, read-only
ATHENA_WORKGROUP = "payments-lakehouse"

# Business questions answered from the gold star schema on AWS, after each run.
GOLD_REPORTS = {
    "Success rate by payment method": """
        SELECT m.method_type, count(*) AS attempts,
               round(100.0 * avg(f.is_captured), 1) AS success_pct
        FROM gold.fact_payments f JOIN gold.dim_payment_method m USING (payment_method_sk)
        GROUP BY 1 ORDER BY 2 DESC""",
    "Fee revenue by pricing plan (the plan in force at payment time)": """
        SELECT d.pricing_plan, count(*) AS captured_payments,
               CAST(sum(f.fee_paise) / 100.0 AS DECIMAL(18, 2)) AS fees_inr
        FROM gold.fact_payments f JOIN gold.dim_merchant d USING (merchant_sk)
        WHERE f.is_captured = 1
        GROUP BY 1 ORDER BY 3 DESC""",
    "Last 7 business days (India time)": """
        SELECT d.date, d.day_name, count(*) AS attempts, sum(f.is_captured) AS captured,
               CAST(sum(f.amount_paise) / 100.0 AS DECIMAL(18, 2)) AS gross_inr
        FROM gold.fact_payments f JOIN gold.dim_date d USING (date_key)
        GROUP BY 1, 2 ORDER BY 1 DESC LIMIT 7""",
}


def glue_task(task_id, job, **script_args):
    """A task that runs one of the project's Glue jobs (infra/glue_jobs.tf) and waits."""
    return GlueJobOperator(
        task_id=task_id,
        job_name=f"payments-lakehouse-{job}",
        script_args={f"--{name.replace('_', '-')}": value for name, value in script_args.items()},
        aws_conn_id=AWS_CONN,
        wait_for_completion=True,
        verbose=True,                   # copy the job's output into the task log
        job_poll_interval=15,
        stop_job_run_on_kill=True,      # cancelling the task also stops the billed Glue run
        resume_glue_job_on_retry=True,  # a retry waits for a still-running run instead of starting another
    )


def new_batch_id():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def batch_was_exported(batch_id):
    """False (skip the rest) when the export found no new rows and wrote no manifest."""
    exists = (LANDING / "_manifests" / f"{batch_id}.json").exists()
    print(f"batch {batch_id}: {'new rows exported' if exists else 'nothing new, skipping the AWS run'}")
    return exists


def upload_batch(batch_id):
    """Upload one landing batch to S3; the manifest goes last.

    load_bronze.py starts from the manifest, so a batch is only visible to it
    once all its data files are already in S3.
    """
    hook = S3Hook(aws_conn_id=AWS_CONN)
    bucket = os.environ["AWS_LAKEHOUSE_BUCKET"]
    manifest = LANDING / "_manifests" / f"{batch_id}.json"
    uploaded = 0
    for table, info in json.loads(manifest.read_text())["tables"].items():
        if not info["rows"]:
            continue
        for path in sorted((LANDING / table / f"batch_id={batch_id}").glob("*.parquet")):
            hook.load_file(str(path), key=f"landing/bronze/{table}/batch_id={batch_id}/{path.name}",
                           bucket_name=bucket, replace=True)
            uploaded += 1
    hook.load_file(str(manifest), key=f"landing/bronze/_manifests/{batch_id}.json",
                   bucket_name=bucket, replace=True)
    print(f"uploaded {uploaded} data files and the manifest of batch {batch_id}")


def report_from_gold():
    """Run the GOLD_REPORTS queries in Athena and print the results in the task log."""
    hook = AthenaHook(aws_conn_id=AWS_CONN)
    for title, sql in GOLD_REPORTS.items():
        query_id = hook.run_query(sql, query_context={"Catalog": "AwsDataCatalog", "Database": "gold"},
                                  result_configuration={}, workgroup=ATHENA_WORKGROUP)
        state = hook.poll_query_status(query_id, sleep_time=1)
        if state != "SUCCEEDED":
            raise RuntimeError(f"{title}: Athena query {query_id} ended {state}")
        rows = [[c.get("VarCharValue", "") for c in r["Data"]]
                for r in hook.get_query_results(query_id)["ResultSet"]["Rows"]]
        widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
        print(f"\n{title}")
        for i, row in enumerate(rows):
            print("  " + "  ".join(v.rjust(w) if i and v.replace(".", "").isdigit() else v.ljust(w)
                                   for v, w in zip(row, widths)))


AWS_DEFAULTS = {**DEFAULTS, "execution_timeout": timedelta(minutes=20)}

with DAG(
    dag_id="lakehouse_aws",
    description="Ship new bronze rows to AWS, run load -> silver -> gold -> checks on Glue, report via Athena",
    schedule=None,  # manual: each run costs a few cents of Glue time
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=AWS_DEFAULTS,
    tags=["lakehouse", "aws"],
) as aws_dag:
    batch_id = "{{ ti.xcom_pull(task_ids='choose_batch_id') }}"

    choose = PythonOperator(task_id="choose_batch_id", python_callable=new_batch_id)
    export = spark_task("export_new_bronze_rows", "export_bronze.py", "--batch-id", batch_id, landing=True)
    anything_new = ShortCircuitOperator(task_id="new_rows_to_ship", python_callable=batch_was_exported,
                                        op_args=[batch_id])
    upload = PythonOperator(task_id="upload_batch_to_s3", python_callable=upload_batch, op_args=[batch_id])
    load = glue_task("load_bronze", "load-bronze", batch_id=batch_id)
    silver = glue_task("build_silver", "silver-cdc")
    gold = glue_task("build_gold", "gold-star")
    matches_full_rebuild = glue_task("check_incremental_matches_full_rebuild", "gold-check")
    data_quality = glue_task("data_quality_checks", "dq-checks")
    report = PythonOperator(task_id="report_from_gold_with_athena", python_callable=report_from_gold)

    choose >> export >> anything_new >> upload >> load >> silver >> gold
    gold >> [matches_full_rebuild, data_quality] >> report

with DAG(
    dag_id="lakehouse_aws_maintenance",
    description="Iceberg housekeeping on AWS (Glue), one layer at a time",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=AWS_DEFAULTS,
    tags=["lakehouse", "aws", "maintenance"],
) as aws_maintenance_dag:
    previous = None
    for layer in ["bronze", "silver", "gold"]:
        task = glue_task(f"maintain_{layer}", "maintenance", layer=layer)
        if previous:
            previous >> task
        previous = task
