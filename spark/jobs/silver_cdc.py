"""Build silver tables: the current state of every source row, from bronze CDC events.

    lakehouse.bronze.<table>  (every change event)
        -> lakehouse.silver.<table>  (one row per primary key)

Each micro-batch of new bronze events is reduced to the newest event per key,
ordered by its position in the Postgres WAL (source.lsn), then MERGEd:

    newer than what silver has   -> update the row (or mark it deleted)
    same or older                -> ignore (a Debezium duplicate or a replay)
    key not in silver yet        -> insert

Ignoring same-or-older events makes the MERGE idempotent: replaying any range
of bronze, even all of it, leaves silver unchanged. So a crashed batch can
simply run again.

Silver also fixes types (Debezium sends timestamps as ISO-8601 strings) and
removes direct personal data from customers: name, email and phone are
replaced by a keyed hash of the email and the last 4 phone digits.

    spark-submit silver_cdc.py                  # run continuously
    spark-submit silver_cdc.py --available-now  # process new bronze rows, then stop
"""
import argparse
import hashlib
import hmac
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import partitioning
from pyspark.sql.types import StringType

# table -> (primary key, partition column or None)
TABLES = {
    "merchants": ("merchant_id", None),
    "customers": ("customer_id", None),
    "payment_methods": ("payment_method_id", None),
    "payments": ("payment_id", "created_at"),
    "refunds": ("refund_id", "created_at"),
}
BRONZE, SILVER = "lakehouse.bronze", "lakehouse.silver"
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "/checkpoints/silver")
CUSTOMER_PII = ("full_name", "email", "phone")

TABLE_PROPERTIES = {
    "format-version": "2",
    # Updates write small "row X of file Y is deleted" files instead of rewriting
    # whole data files: cheap frequent MERGEs, at the cost of periodic compaction.
    "write.merge.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.delete.mode": "merge-on-read",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "100",
}


def pii_hasher():
    """HMAC-SHA256 of a normalised value, keyed by PII_HASH_KEY.

    A plain SHA-256 of an email can be reversed by hashing a list of known
    emails; without the key that isn't possible. It's a Python UDF rather than
    sha2(concat(key, email)) so the key never appears in the query plan (which
    the Spark UI and logs show).
    """
    key = os.environ["PII_HASH_KEY"].encode()

    def keyed_hash(value):
        if value is None:
            return None
        return hmac.new(key, value.strip().lower().encode(), hashlib.sha256).hexdigest()

    return F.udf(keyed_hash, StringType())


def silver_columns(df, image, table, email_hash):
    """Typed silver columns from a row-image struct column.

    Works on a Debezium `after` struct and on a Postgres row read over JDBC
    (wrapped in a struct), so the reconciliation applies the exact same rules.
    """
    columns = []
    for name in df.schema[image].dataType.fieldNames():
        if table == "customers" and name in CUSTOMER_PII:
            continue
        col = F.col(f"{image}.{name}")
        if name.endswith("_at"):
            col = col.cast("timestamp")  # '2026-11-21T15:26:00.497069Z' -> timestamp (UTC)
        columns.append(col.alias(name))
    if table == "customers":
        columns += [email_hash(F.col(f"{image}.email")).alias("email_hash"),
                    F.substring(F.col(f"{image}.phone"), -4, 4).alias("phone_last4")]
    return columns


def latest_per_key(events, table, email_hash):
    """Reduce change events to one row per key: the newest event, ready to MERGE."""
    pk, _ = TABLES[table]
    events = events.select(
        "*",
        # Deletes have no `after`; their `before` holds the real primary key.
        F.coalesce(F.col(f"after.{pk}"), F.col(f"before.{pk}")).alias("_key"),
        F.col("source.lsn").alias("_lsn"),
        F.timestamp_millis(F.col("source.ts_ms")).alias("_source_ts"),
    )
    newest_first = Window.partitionBy("_key").orderBy(F.desc("_lsn"), F.desc("kafka_offset"))
    whole_key = newest_first.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    latest = (events
              .withColumn("_rank", F.row_number().over(newest_first))
              # The newest full row image. Normally the newest event's `after`;
              # if the newest event is a delete, the last image before it.
              .withColumn("image", F.first("after", ignorenulls=True).over(whole_key))
              .where("_rank = 1"))
    is_delete = F.col("op") == "d"
    return (latest
            .select(*silver_columns(latest, "image", table, email_hash),
                    F.col("op").alias("_op"), "_lsn", "_source_ts",
                    is_delete.alias("_is_deleted"),
                    F.when(is_delete, F.col("_source_ts")).alias("_deleted_at"),
                    F.current_timestamp().alias("_silver_updated_at"),
                    F.col("image").isNotNull().alias("_has_image"),
                    "_key")
            .withColumn(pk, F.col("_key")))  # also correct when the image is missing


def merge_sql(table, source_view, columns):
    pk, _ = TABLES[table]
    target = f"{SILVER}.{table}"
    newer = "s._lsn > t._lsn"
    mark_deleted = ", ".join(f"t.{c} = s.{c}" for c in
                             ["_op", "_lsn", "_source_ts", "_is_deleted", "_deleted_at", "_silver_updated_at"])
    update_all = ", ".join(f"t.`{c}` = s.`{c}`" for c in columns)
    insert_cols = ", ".join(f"`{c}`" for c in columns)
    insert_vals = ", ".join(f"s.`{c}`" for c in columns)
    return f"""
        MERGE INTO {target} t
        USING {source_view} s
        ON t.{pk} = s.{pk}
        WHEN MATCHED AND {newer} AND NOT s._has_image THEN UPDATE SET {mark_deleted}
        WHEN MATCHED AND {newer} THEN UPDATE SET {update_all}
        WHEN NOT MATCHED AND s._has_image THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """


def ensure_table(spark, table, schema):
    """Create the silver table, or add columns that appeared in bronze since."""
    name = f"{SILVER}.{table}"
    if not spark.catalog.tableExists(name):
        writer = spark.createDataFrame([], schema).writeTo(name).using("iceberg")
        for key, value in TABLE_PROPERTIES.items():
            writer = writer.tableProperty(key, value)
        _, partition_col = TABLES[table]
        if partition_col:
            writer = writer.partitionedBy(partitioning.days(partition_col))
        writer.create()
        print(f"created {name}", flush=True)
        return
    existing = set(spark.table(name).columns)
    for field in schema.fields:
        if field.name not in existing:
            spark.sql(f"ALTER TABLE {name} ADD COLUMN `{field.name}` {field.dataType.simpleString()}")
            print(f"{name}: added column {field.name}", flush=True)


def latest_snapshot(session, table):
    """The table's current snapshot: what the `main` branch points to.

    Not simply the most recently committed snapshot: after a rollback, the
    current snapshot is an older one.
    """
    name = f"{SILVER}.{table}"
    return session.sql(f"SELECT s.snapshot_id, s.summary FROM {name}.refs r "
                       f"JOIN {name}.snapshots s ON r.snapshot_id = s.snapshot_id "
                       "WHERE r.name = 'main'").first()


def start_query(spark, table, args, email_hash):
    bronze_name = f"{BRONZE}.{table}"
    bronze_schema = spark.table(bronze_name).schema
    empty = latest_per_key(spark.createDataFrame([], bronze_schema), table, email_hash)
    columns = [c for c in empty.columns if c not in ("_has_image", "_key")]
    ensure_table(spark, table, empty.select(*columns).schema)

    def process(batch, batch_id):
        # A new column in bronze isn't visible to a running stream. Stop, and the
        # restart adds it to silver (ensure_table) before reading again.
        session = batch.sparkSession
        if session.table(bronze_name).schema != bronze_schema:
            raise RuntimeError(f"{bronze_name} schema changed; restarting to pick it up")
        batch.persist()  # read twice below: the emptiness check and the MERGE
        try:
            if batch.isEmpty():
                return
            before = latest_snapshot(session, table)
            view = f"silver_batch_{table}"
            latest_per_key(batch, table, email_hash).createOrReplaceTempView(view)
            session.sql(merge_sql(table, view, columns))
            after = latest_snapshot(session, table)
            if after is None or (before and after["snapshot_id"] == before["snapshot_id"]):
                print(f"silver_{table} batch {batch_id}: no changes (all events already applied)", flush=True)
            else:
                s = after["summary"]
                print(f"silver_{table} batch {batch_id}: +{s.get('added-records', 0)} rows, "
                      f"+{s.get('added-position-deletes', 0)} position deletes", flush=True)
        finally:
            batch.unpersist()

    stream = (spark.readStream.format("iceberg")
              .option("streaming-max-rows-per-micro-batch", args.max_rows_per_batch)
              .load(bronze_name))
    writer = (stream.writeStream
              .queryName(f"silver_{table}")
              .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{table}")
              .foreachBatch(process))
    writer = (writer.trigger(availableNow=True) if args.available_now
              else writer.trigger(processingTime=f"{args.trigger_seconds} seconds"))
    return writer.start()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--available-now", action="store_true",
                        help="process new bronze rows, then stop")
    parser.add_argument("--trigger-seconds", type=int, default=60)
    parser.add_argument("--max-rows-per-batch", type=int, default=1_000_000)
    parser.add_argument("--tables", nargs="+", choices=list(TABLES), default=list(TABLES),
                        help="process only these tables (e.g. to backfill one table)")
    args = parser.parse_args()

    spark = (SparkSession.builder.appName("silver-cdc")
             .config("spark.sql.catalog.lakehouse.jdbc.password", os.environ["ICEBERG_CATALOG_PASSWORD"])
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER}")
    email_hash = pii_hasher()

    queries = [start_query(spark, table, args, email_hash) for table in args.tables]
    for q in queries:
        print(f"started {q.name}", flush=True)
    if args.available_now:
        for q in queries:
            q.awaitTermination()
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
