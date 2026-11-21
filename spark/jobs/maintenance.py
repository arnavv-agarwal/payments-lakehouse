"""Iceberg table maintenance for one layer: the housekeeping streaming tables need.

For each table:
  1. rewrite_data_files        compact small files (streaming writes many) into
                               larger ones; in silver, also rewrite files that
                               have position deletes applied to them
  2. rewrite_position_delete_files   (silver) clean up remaining delete files
  3. expire_snapshots          drop snapshots older than a day (keeping the last
                               20), and the data files only they referenced
  4. remove_orphan_files       delete files no snapshot references, e.g. from
                               failed writes, if older than 3 days

Snapshot retention is a safety margin, not just a storage setting: silver reads
bronze incrementally, snapshot by snapshot. If bronze expired snapshots that
silver hasn't read yet, the silver stream would fail. Silver trails by about a
minute, so a day is plenty.

    spark-submit maintenance.py --layer bronze|silver|gold
"""
import argparse
import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

LAYERS = {
    "bronze": ["merchants", "customers", "payment_methods", "payments", "refunds"],
    "silver": ["merchants", "customers", "payment_methods", "payments", "refunds"],
    "gold": ["fact_payments", "fact_refunds", "dim_merchant", "dim_customer",
             "dim_payment_method", "dim_date"],
}
KEEP_SNAPSHOTS_HOURS = 24
ORPHAN_MIN_AGE_DAYS = 3  # never touch files a running write might still commit


def file_stats(spark, table):
    """(data files, delete files, snapshots) in the current table state.

    The `files` metadata table lists both: content 0 = data, 1/2 = position/equality deletes.
    """
    row = spark.sql(f"SELECT count_if(content = 0) AS data, count_if(content > 0) AS deletes "
                    f"FROM {table}.files").first()
    snapshots = spark.sql(f"SELECT count(*) FROM {table}.snapshots").first()[0]
    return row["data"], row["deletes"], snapshots


def maintain(spark, layer, table):
    name = f"lakehouse.{layer}.{table}"
    before = file_stats(spark, name)
    options = {"min-input-files": "5", "partial-progress.enabled": "true"}
    if layer == "silver":
        options["delete-file-threshold"] = "1"  # rewrite files that have deletes against them
    opts = ", ".join(f"'{k}', '{v}'" for k, v in options.items())
    spark.sql(f"CALL lakehouse.system.rewrite_data_files(table => '{name}', options => map({opts}))")
    if layer == "silver":
        spark.sql(f"CALL lakehouse.system.rewrite_position_delete_files(table => '{name}')")
    # Procedure arguments must be literals: an expression like
    # current_timestamp() - INTERVAL ... fails to bind, so compute cutoffs here.
    now = datetime.now(timezone.utc)
    expire_before = (now - timedelta(hours=KEEP_SNAPSHOTS_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
    orphan_before = (now - timedelta(days=ORPHAN_MIN_AGE_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    spark.sql(f"""CALL lakehouse.system.expire_snapshots(
                    table => '{name}', older_than => TIMESTAMP '{expire_before}', retain_last => 20)""")
    # prefix_listing: list files through Iceberg's S3FileIO. The default lists
    # through Hadoop's FileSystem, which has no "s3" scheme in this image.
    orphans = spark.sql(f"""CALL lakehouse.system.remove_orphan_files(
                    table => '{name}', older_than => TIMESTAMP '{orphan_before}',
                    prefix_listing => true)""").count()
    after = file_stats(spark, name)
    print(f"{name:40} data files {before[0]:>4} -> {after[0]:<4} delete files {before[1]:>3} -> {after[1]:<3} "
          f"snapshots {before[2]:>4} -> {after[2]:<4} orphans removed {orphans}", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", required=True, choices=list(LAYERS))
    args = parser.parse_args()
    spark = (SparkSession.builder.appName(f"maintenance-{args.layer}")
             .config("spark.sql.catalog.lakehouse.jdbc.password", os.environ["ICEBERG_CATALOG_PASSWORD"])
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    for table in LAYERS[args.layer]:
        maintain(spark, args.layer, table)
    spark.stop()


if __name__ == "__main__":
    main()
