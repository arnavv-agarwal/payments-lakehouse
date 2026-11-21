"""Orchestration for the payments lakehouse.

Airflow schedules and monitors; it doesn't compute. Every task starts a
short-lived container from the lakehouse Spark image and waits for it, the
local equivalent of submitting a job to EMR Serverless (Phase 2). The
streaming jobs (bronze, silver) run continuously as their own services; Airflow
handles the batch work around them:

    lakehouse_gold         every 10 min   build gold -> verify it two ways
    lakehouse_maintenance  daily 02:30 IST  compact + clean up bronze -> silver -> gold
"""
import os
from datetime import timedelta

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG
from docker.types import Mount

SPARK_IMAGE = "payments-lakehouse/spark:4.1.4-iceberg-1.12.0"
JOBS = "/opt/lakehouse/jobs"
# Passed as private_environment: unlike `environment`, these are not shown in
# the Airflow UI's rendered-template view or in logs.
SECRETS = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "ICEBERG_CATALOG_PASSWORD",
           "PII_HASH_KEY", "DEBEZIUM_PASSWORD"]


def spark_task(task_id, script, *args, py_files=None, **kwargs):
    """A task that runs one Spark job in its own container."""
    command = ["/opt/spark/bin/spark-submit"]
    if py_files:
        command += ["--py-files", f"{JOBS}/{py_files}"]
    command += [f"{JOBS}/{script}", *args]
    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,
        command=command,
        docker_url=os.environ.get("DOCKER_HOST", "tcp://docker-proxy:2375"),
        network_mode=os.environ.get("LAKEHOUSE_NETWORK", "payments-lakehouse_default"),
        # Bind-mount the job code from the project folder (a path on the Docker
        # host, not inside the Airflow container), like the streaming services do.
        mounts=[Mount(source=f"{os.environ.get('LAKEHOUSE_HOST_DIR', '')}/spark/jobs",
                      target=JOBS, type="bind", read_only=True)],
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
