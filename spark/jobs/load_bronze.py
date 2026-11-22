"""Load one landing batch into the bronze Iceberg tables (runs on AWS Glue).

Reads the Parquet files that export_bronze.py wrote and push_landing.sh
uploaded, checks them against the batch manifest, and appends the rows that
bronze doesn't have yet.

Idempotent: rows are identified by their Kafka coordinates (kafka_partition,
kafka_offset). Rows already in the table are skipped (an anti-join), so
loading the same batch twice, or two batches that overlap after an export
retry, never creates duplicates.

Each append commit carries the batch id in its snapshot summary
(`landing-batch-id`), and every load is recorded in ops.bronze_loads, so any
bronze row can be traced back to the batch that brought it.

    load_bronze.py --landing s3://<bucket>/landing --batch-id 20261122T070000Z
    (locally, for testing: --landing file:///landing --namespace bronze_loadtest)
"""
import argparse
import json

from pyspark.sql import functions as F

import lakehouse_env

KEY = ["kafka_partition", "kafka_offset"]


def read_manifest(spark, landing, batch_id):
    path = f"{landing}/bronze/_manifests/{batch_id}.json"
    text = spark.read.text(path, wholetext=True).first()[0]
    return json.loads(text)


def ensure_table(spark, target, source_view):
    """Create the bronze table (empty, same schema as the batch) if it doesn't exist.

    Same layout as the local bronze tables: partitioned by day of the Kafka
    timestamp. Format version 2, because Athena can't read Iceberg v3.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target}
        USING iceberg
        PARTITIONED BY (days(kafka_timestamp))
        TBLPROPERTIES ('format-version' = '2',
                       'write.spark.accept-any-schema' = 'true')
        AS SELECT * FROM {source_view} WHERE false
    """)


def load_table(spark, args, table, expected_rows):
    src = spark.read.parquet(f"{args.landing}/bronze/{table}/batch_id={args.batch_id}")
    rows_in_batch = src.count()
    if rows_in_batch != expected_rows:
        raise RuntimeError(f"{table}: batch has {rows_in_batch} rows, manifest says {expected_rows}")

    target = f"{args.catalog}.{args.namespace}.{table}"
    src.createOrReplaceTempView("batch")
    ensure_table(spark, target, "batch")

    existing = spark.table(target).select(*KEY)
    # A join on a column list moves those columns to the front; restore the
    # batch's column order, which the table write expects.
    new_rows = src.join(existing, KEY, "left_anti").select(*src.columns).cache()
    appended = new_rows.count()
    if appended:
        (new_rows.writeTo(target)
                 .option("mergeSchema", "true")  # a new source column is added, as locally
                 .option("snapshot-property.landing-batch-id", args.batch_id)
                 .append())
    new_rows.unpersist()
    return rows_in_batch, appended


def record_loads(spark, args, results):
    log_table = f"{args.catalog}.{args.ops_namespace}.bronze_loads"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {log_table} (
            batch_id       string,
            table_name     string,
            rows_in_batch  bigint,
            rows_appended  bigint,
            loaded_at      timestamp)
        USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)
    rows = [(args.batch_id, t, n, a) for t, (n, a) in results.items()]
    (spark.createDataFrame(rows, "batch_id string, table_name string, rows_in_batch bigint, rows_appended bigint")
          .withColumn("loaded_at", F.current_timestamp())
          .writeTo(log_table).append())


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--landing", required=True, help="e.g. s3://<bucket>/landing")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--catalog", default="lakehouse")
    parser.add_argument("--namespace", default="bronze")
    parser.add_argument("--ops-namespace", default="ops")
    args = lakehouse_env.parse_args(parser)

    spark = lakehouse_env.spark_session("load-bronze")
    manifest = read_manifest(spark, args.landing, args.batch_id)

    results = {}
    for table, info in manifest["tables"].items():
        if info["rows"] == 0:
            continue
        results[table] = load_table(spark, args, table, info["rows"])
        n, appended = results[table]
        print(f"{table:16s} batch {n:>9,} rows, appended {appended:>9,}, skipped {n - appended:>9,}")

    record_loads(spark, args, results)
    print(f"Batch {args.batch_id} loaded into {args.catalog}.{args.namespace}")


if __name__ == "__main__":
    main()
