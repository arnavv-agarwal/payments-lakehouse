"""Export new bronze rows to Parquet files in a local landing folder.

Phase 2 moves the batch layer to AWS while ingestion (Postgres -> Debezium ->
Kafka -> bronze) stays on the laptop. This job is the hand-off: it writes the
bronze rows that haven't been exported yet as one "landing batch":

    /landing/bronze/<table>/batch_id=<id>/part-*.parquet
    /landing/bronze/_manifests/<id>.json      what the batch contains (rows,
                                              Kafka offset range per partition)

Afterwards `tools/push_landing.sh` uploads the folder to S3 and the Glue job
`load_bronze.py` appends the batch to the bronze tables on AWS.

Which rows are new? Every bronze row has unique Kafka coordinates
(kafka_partition, kafka_offset), and offsets only grow within a partition. The
export remembers the highest offset it exported per partition
(/landing/_state/export_state.json) and takes everything above it next time.
That bookmark is saved last: if the job dies earlier, the next run exports the
same rows again, which is harmless because the load skips rows it already has.

    spark-submit export_bronze.py [--tables payments,refunds] [--batch-id ID]
"""
import argparse
import json
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TABLES = ["merchants", "customers", "payment_methods", "payments", "refunds"]
LANDING = "/landing"
STATE_FILE = f"{LANDING}/_state/export_state.json"


def load_state():
    if not os.path.exists(STATE_FILE):
        return {}
    with open(STATE_FILE) as f:
        return json.load(f)


def write_json(path, data):
    """Write via a temporary file + rename, so a crash never leaves half a file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(tmp, path)


def new_rows_condition(watermarks):
    """Rows above the exported offset in known partitions, and every row in new partitions."""
    if not watermarks:
        return F.lit(True)
    known = [int(p) for p in watermarks]
    cond = ~F.col("kafka_partition").isin(known)
    for partition, offset in watermarks.items():
        cond = cond | ((F.col("kafka_partition") == int(partition)) & (F.col("kafka_offset") > offset))
    return cond


def export_table(spark, table, batch_id, watermarks):
    df = spark.table(f"lakehouse.bronze.{table}").where(new_rows_condition(watermarks))
    # One pass to find the offset range of this batch (also tells us if it's empty).
    ranges = (df.groupBy("kafka_partition")
                .agg(F.count("*").alias("rows"),
                     F.min("kafka_offset").alias("min_offset"),
                     F.max("kafka_offset").alias("max_offset"))
                .collect())
    if not ranges:
        return {"rows": 0, "partitions": {}}

    path = f"{LANDING}/bronze/{table}/batch_id={batch_id}"
    # Batches are small (tens of MB), so one file per table keeps the landing tidy.
    df.coalesce(1).write.mode("errorifexists").parquet(f"file://{path}")

    # Read back what was written: the manifest records what is really on disk.
    written = spark.read.parquet(f"file://{path}").count()
    expected = sum(r["rows"] for r in ranges)
    if written != expected:
        raise RuntimeError(f"{table}: wrote {written} rows, expected {expected}")

    return {
        "rows": written,
        "partitions": {str(r["kafka_partition"]): {"rows": r["rows"],
                                                   "min_offset": r["min_offset"],
                                                   "max_offset": r["max_offset"]}
                       for r in ranges},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--tables", default=",".join(TABLES))
    parser.add_argument("--batch-id", default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    args = parser.parse_args()
    tables = args.tables.split(",")

    spark = (SparkSession.builder.appName("export-bronze")
             .config("spark.sql.catalog.lakehouse.jdbc.password", os.environ["ICEBERG_CATALOG_PASSWORD"])
             .getOrCreate())
    state = load_state()
    manifest = {"batch_id": args.batch_id,
                "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "tables": {}}

    for table in tables:
        info = export_table(spark, table, args.batch_id, state.get(table, {}))
        manifest["tables"][table] = info
        print(f"{table:16s} {info['rows']:>9,} rows")

    total = sum(t["rows"] for t in manifest["tables"].values())
    if total == 0:
        print("Nothing new to export.")
        return

    write_json(f"{LANDING}/bronze/_manifests/{args.batch_id}.json", manifest)

    # Advance the bookmark last.
    for table, info in manifest["tables"].items():
        for partition, r in info["partitions"].items():
            state.setdefault(table, {})[partition] = r["max_offset"]
    write_json(STATE_FILE, state)
    print(f"Batch {args.batch_id}: {total:,} rows -> {LANDING}/bronze (manifest + state saved)")


if __name__ == "__main__":
    main()
