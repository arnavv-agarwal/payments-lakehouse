"""Stream Debezium change events from Kafka into Iceberg bronze tables.

One Structured Streaming query per source table:

    Kafka topic payments.public.<table>  ->  Iceberg table lakehouse.bronze.<table>

Bronze keeps each Debezium envelope as produced (op, before, after, source)
plus where it came from in Kafka (partition, offset, timestamp) and which
schema version wrote it. Later layers deduplicate, order and merge; bronze
only records. Tombstones (null values) are dropped: the delete event before
each one already carries the delete.

Exactly-once: Spark stores the Kafka offsets of every micro-batch in its
checkpoint, and the Iceberg sink records the batch id in each commit, so a
batch that is retried after a crash is never written twice.

    spark-submit bronze_cdc.py                  # run continuously
    spark-submit bronze_cdc.py --available-now  # process what's in Kafka, then stop
"""
import argparse
import json
import os
import urllib.request

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import partitioning

TABLES = ["merchants", "customers", "payment_methods", "payments", "refunds"]
TOPIC_PREFIX = "payments.public"
NAMESPACE = "lakehouse.bronze"
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "/checkpoints/bronze")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SCHEMA_REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

TABLE_PROPERTIES = {
    "format-version": "2",
    # Lets a write add columns that appeared in a newer schema version.
    "write.spark.accept-any-schema": "true",
    # Every micro-batch commit writes a new metadata.json; keep only the last 100.
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "100",
}


def schema_versions(subject):
    """All registered versions of a subject as [(schema_id, schema_json)], oldest first."""
    def get(path):
        with urllib.request.urlopen(f"{SCHEMA_REGISTRY}{path}", timeout=10) as resp:
            return json.load(resp)

    return [(v["id"], v["schema"])
            for v in (get(f"/subjects/{subject}/versions/{n}")
                      for n in get(f"/subjects/{subject}/versions"))]


def decode_confluent_avro(value: Column, versions) -> tuple[Column, Column]:
    """Decode Confluent-framed Avro: 1 magic byte (0), a 4-byte schema id, the Avro body.

    Each message is decoded with the schema that wrote it (its id) and resolved
    to the newest schema, so every row comes out with the same struct type even
    across schema versions. An id that wasn't registered when the job started
    fails the query on purpose: decoding with the wrong schema would silently
    produce garbage. A restart picks up the new version.

    Returns (decoded envelope, schema id) columns.
    """
    magic_ok = F.hex(F.substring(value, 1, 1)) == F.lit("00")
    schema_id = F.conv(F.hex(F.substring(value, 2, 4)), 16, 10).cast("int")
    body = F.substring(value, 6, 2_147_483_647)
    newest = versions[-1][1]

    decoded = F.when(~magic_ok, F.raise_error(F.lit("Not Confluent Avro: magic byte is not 0")))
    for sid, writer_schema in versions:
        # writer schema = how the bytes were encoded; avroSchema = what we want back.
        decoded = decoded.when(schema_id == sid, from_avro(body, writer_schema, {"avroSchema": newest}))
    decoded = decoded.otherwise(F.raise_error(F.concat(
        F.lit("Unknown schema id "), schema_id.cast("string"),
        F.lit(": a new schema version was registered after this job started; restart it."))))
    return decoded, schema_id


def bronze_stream(spark, table, max_offsets):
    topic = f"{TOPIC_PREFIX}.{table}"
    versions = schema_versions(f"{topic}-value")
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", topic)
           .option("startingOffsets", "earliest")  # only used on the very first run
           .option("maxOffsetsPerTrigger", max_offsets)  # caps a batch, e.g. the snapshot backlog
           .option("failOnDataLoss", "true")  # fail if Kafka deleted offsets we never read
           .load()
           .where(F.col("value").isNotNull()))  # drop tombstones

    envelope, schema_id = decode_confluent_avro(F.col("value"), versions)
    events = raw.select(
        envelope.alias("envelope"),
        schema_id.alias("schema_id"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).select("envelope.*", "schema_id", "kafka_partition", "kafka_offset", "kafka_timestamp",
             F.current_timestamp().alias("ingested_at"))
    return events, len(versions)


def ensure_table(spark, name, schema):
    if spark.catalog.tableExists(name):
        return
    writer = spark.createDataFrame([], schema).writeTo(name).using("iceberg")
    for key, value in TABLE_PROPERTIES.items():
        writer = writer.tableProperty(key, value)
    # Daily partitions on the Kafka timestamp: a replay lands rows in the same
    # partitions as the first load, unlike partitioning on ingestion time.
    writer.partitionedBy(partitioning.days("kafka_timestamp")).create()
    print(f"created {name}", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--available-now", action="store_true",
                        help="process everything currently in Kafka, then stop")
    parser.add_argument("--trigger-seconds", type=int, default=30)
    parser.add_argument("--max-offsets-per-trigger", type=int, default=250_000)
    parser.add_argument("--tables", nargs="+", choices=TABLES, default=TABLES,
                        help="process only these tables (e.g. to backfill one table)")
    args = parser.parse_args()

    spark = (SparkSession.builder.appName("bronze-cdc")
             .config("spark.sql.catalog.lakehouse.jdbc.password", os.environ["ICEBERG_CATALOG_PASSWORD"])
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")

    queries = []
    for table in args.tables:
        name = f"{NAMESPACE}.{table}"
        events, n_versions = bronze_stream(spark, table, args.max_offsets_per_trigger)
        ensure_table(spark, name, events.schema)
        writer = (events.writeStream
                  .format("iceberg")
                  .outputMode("append")
                  .queryName(f"bronze_{table}")
                  .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{table}")
                  .option("fanout-enabled", "true")  # write to many partitions without sorting first
                  .option("mergeSchema", "true"))    # evolve the table when the source adds columns
        writer = (writer.trigger(availableNow=True) if args.available_now
                  else writer.trigger(processingTime=f"{args.trigger_seconds} seconds"))
        queries.append(writer.toTable(name))
        print(f"started bronze_{table} ({n_versions} schema version(s))", flush=True)

    if args.available_now:
        for q in queries:
            q.awaitTermination()
    else:
        # If any query fails, stop: the container restarts and every query
        # resumes from its checkpoint.
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
