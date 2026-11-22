"""What differs between running a job locally (Docker) and on AWS Glue, in one place.

The same job files run in both places. Three things differ:

1. The Iceberg catalog. Locally it's a JDBC catalog in Postgres and needs a
   password; on Glue it's the Glue Data Catalog and access comes from the
   job's IAM role. Table names are the same (lakehouse.silver.payments, ...).
2. Arguments and exit codes. Glue passes every argument as "--name value"
   (no bare flags), adds its own (--JOB_ID, --JOB_RUN_ID, --TempDir, ...), and
   treats sys.exit(0) as a failure.
3. Secrets. Locally they come from .env as environment variables; on Glue
   from AWS Systems Manager Parameter Store (encrypted at rest).
"""
import argparse
import os
import sys

from pyspark.sql import SparkSession

ON_GLUE = "--JOB_RUN_ID" in sys.argv


def spark_session(app_name):
    builder = SparkSession.builder.appName(app_name)
    password = os.environ.get("ICEBERG_CATALOG_PASSWORD")
    if password:
        builder = builder.config("spark.sql.catalog.lakehouse.jdbc.password", password)
    return builder.getOrCreate()


def _boolean(value):
    if value.lower() in ("true", "1", "yes"):
        return True
    if value.lower() in ("false", "0", "no"):
        return False
    raise argparse.ArgumentTypeError(f"expected true or false, got {value!r}")


def add_flag(parser, name, help=None):
    """A yes/no option that works as `--name` (locally) and `--name true` (Glue)."""
    parser.add_argument(name, nargs="?", const=True, default=False, type=_boolean, help=help)


def parse_args(parser):
    """Parse arguments. On Glue, ignore the ones Glue adds; locally, reject unknown ones."""
    args, unknown = parser.parse_known_args()
    if unknown and not ON_GLUE:
        parser.error(f"unrecognized arguments: {' '.join(unknown)}")
    return args


def finish(failed):
    """End a job that reports pass/fail: exit 1 on failure, otherwise just return.

    Not sys.exit(0) on success: Glue 6.0 reports any SystemExit, even with
    code 0, as a FAILED run. Returning normally works in both places.
    """
    if failed:
        sys.exit(1)


def secret(env_var, ssm_parameter=None):
    """A secret from the environment (local) or from Parameter Store (Glue).

    Fetched once, on the driver. Never printed or put into SQL.
    """
    value = os.environ.get(env_var)
    if value:
        return value
    if ssm_parameter:
        import boto3  # available on Glue; not needed locally
        response = boto3.client("ssm").get_parameter(Name=ssm_parameter, WithDecryption=True)
        return response["Parameter"]["Value"]
    raise KeyError(f"set {env_var}, or pass the Parameter Store name that holds it")
