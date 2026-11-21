# Phase 1, Step 4: Spark Structured Streaming into Iceberg (bronze)

## What this step builds

A PySpark streaming job that copies every CDC event from Kafka into
**Apache Iceberg** tables: the bronze layer of the lakehouse. Bronze is a
faithful record of what happened (every insert, update, delete), not yet the
current state of anything.

```
Kafka topics ──► Spark Structured Streaming ──► Iceberg tables  lakehouse.bronze.<table>
(Avro, 5 topics)  spark/jobs/bronze_cdc.py      │
                  1 streaming query per table   ├─ data + metadata files ─► object store (S3 API)
                  micro-batch every 30 s        └─ "current metadata" pointer ─► catalog database
```

| Service | Image | Port | Role |
|---|---|---|---|
| spark-bronze | built from `spark/Dockerfile` (Spark 4.1.4, Iceberg 1.12.0) | 4040 (Spark UI) | Runs the streaming job; restarts itself on failure |
| object-store | `rustfs/rustfs:1.0.5` | 9000 (S3 API), 9001 (console) | S3-compatible storage for all lakehouse files |
| object-store-init | `amazon/aws-cli:2.39.6` | | Creates the `lakehouse` bucket once, then exits |
| iceberg-catalog | `postgres:17.12` | | Iceberg JDBC catalog: table name → current metadata file |

**Local stand-ins for AWS.** The object store is an S3 stand-in, and the JDBC catalog
plays the role AWS Glue will play later. The job code doesn't change between them,
only the catalog settings in `spark/conf/spark-defaults.conf`. **Why RustFS and not
MinIO:** MinIO stopped publishing container images (the Docker Hub repository no
longer exists), and RustFS is an Apache-2.0 store with the same S3 API.

| File | Purpose |
|---|---|
| `spark/Dockerfile` | Spark + Kafka, Avro, Iceberg, S3 and Postgres-driver jars, resolved at build time |
| `spark/conf/spark-defaults.conf` | The `lakehouse` catalog definition (no secrets) |
| `spark/jobs/bronze_cdc.py` | The streaming job |
| `spark/sql.sh` | Spark SQL shell against the lakehouse |
| `postgres/migrations/001_merchants_add_gstin.sql` | First schema migration (used for the schema-evolution test) |
| `postgres/init/03_apply_migrations.sh` | Applies migrations on a fresh database |

## Measured results

| Test | Result |
|---|---|
| Backfill: whole Kafka backlog with `--available-now` | 730,399 events decoded and written in **~11 s** (payments in 3 batches of ≤ 250K) |
| Row counts vs Kafka | bronze = Kafka events minus tombstones, **exactly**, for all 5 tables |
| **Crash**: SIGKILL the Python driver mid-stream, generator still writing | container restarted, every query resumed from its checkpoint |
| **Schema change**: `ALTER TABLE merchants ADD COLUMN gstin` during streaming | Debezium registered schema v2; the job stopped on the unknown schema id, restarted, and the Iceberg table gained `after.gstin` |
| Duplicates after both incidents | **0**: 664,930 payment rows = 664,930 distinct `(partition, offset)` |
| Time travel | `VERSION AS OF <first batch>` returns 249,998 rows; now 664,930 |
| Compaction while streaming | merchants: 22 files → 1, rows unchanged, streaming commits unaffected |

## Key concepts

**Structured Streaming = repeated small batch jobs.** Every 30 s (the trigger),
Spark asks Kafka for new offsets, processes that range as one batch, and commits.
`maxOffsetsPerTrigger` caps a batch, so the 660K-event snapshot backlog was split
into 3 batches instead of one huge one. `--available-now` processes everything
currently available in batches, then stops: streaming code on a schedule, which
is how you run it cheaply from Airflow later.

**Exactly-once = replayable source + checkpoint + idempotent sink.**
1. Kafka can re-read any offset range.
2. The checkpoint (`/checkpoints/bronze/<table>`) records which offsets each batch covers, *before* processing it.
3. The Iceberg sink writes the batch id (`spark.sql.streaming.epochId`) and query id into the commit. After a crash, the replayed batch is recognised as already committed and skipped.

That's why the crash test produced 0 duplicates, even though Debezium itself is
only at-least-once (its duplicates have *different* offsets, so they're kept in
bronze, as they should be, and removed in silver).

**Decoding Confluent Avro.** Each message value is: magic byte `0`, a 4-byte
schema id, then Avro. Spark's `from_avro` only understands the Avro part. The job:
1. fetches every registered version of the topic's schema from Schema Registry at startup,
2. strips the 5-byte header and decodes each message with **the schema that wrote it** (matched by id),
3. resolves it to the **newest** schema (`avroSchema` option), so all rows get one struct type,
4. stops with a clear error on an unknown id. Decoding bytes with the wrong schema doesn't always fail: it can silently produce wrong values.

**Schema evolution end to end.** Postgres `ADD COLUMN` → Debezium registers v2
(BACKWARD compatible: new optional field) → the bronze query stops on id 13 →
Docker restarts the job → it loads v1 and v2 → `mergeSchema` +
`write.spark.accept-any-schema` add `after.gstin` to the Iceberg table. Old rows
read it as NULL. Nothing was rewritten.

**What Iceberg adds over plain Parquet files.**
- **Catalog**: one atomic pointer per table to its current `metadata.json`. A commit swaps that pointer, so readers see a batch completely or not at all.
- **Snapshots**: every commit is a snapshot, which gives you **time travel** (`VERSION AS OF`, `TIMESTAMP AS OF`) and rollback.
- **Hidden partitioning**: `days(kafka_timestamp)` is a transform. Queries filter on the timestamp and Iceberg prunes files, with no `dt=` column for users to remember.
- **Schema evolution by column id**, not position or name, so add/rename/reorder is safe.
- **Optimistic concurrency**: compaction committed while the stream kept appending.

**The small-files cost.** 30-second batches × 5 tables produce many tiny files
(merchants: 22 files averaging 15 KB). Every file is a request when reading, so
reads slow down over time. Fix: periodic compaction, `rewrite_data_files`
(22 → 1 here), plus `expire_snapshots` to drop old snapshots and their unreferenced files.
Longer triggers mean fewer, bigger files but higher latency. That's a trade-off
to state in interviews.

## Design choices

| Choice | Reason |
|---|---|
| Bronze keeps the full envelope (`op`, `before`, `after`, `source`) + Kafka partition/offset/timestamp + `schema_id` | Silver can deduplicate (offset, LSN), order (LSN), and audit (`schema_id`) without going back to Kafka |
| Drop tombstones | The `d` event before each tombstone already records the delete |
| One streaming query per table, one checkpoint each | Tables progress and recover independently; one bad table doesn't rewind the others |
| Partition by `days(kafka_timestamp)`, not ingestion time | Re-ingesting from Kafka puts rows in the same partitions as the first time |
| `failOnDataLoss=true` | If Kafka deleted offsets before Spark read them, stop instead of silently skipping data |
| Fail on unknown schema id + Docker `restart: unless-stopped` | Never decode with the wrong schema; recovery is automatic |
| Jars baked into the image, duplicates of Spark's own jars removed | No downloads at startup; no two versions of one library on the classpath |
| Separate Postgres for the Iceberg catalog | The payments database belongs to the application, not to the lakehouse |
| Checkpoints on a Docker volume | Production keeps them on S3 next to the data |

## Gotchas found in this step

1. **MinIO images no longer exist** on Docker Hub, and the quay.io pull failed. Swapped to RustFS (same S3 API).
2. **Iceberg has no runtime for Spark 4.2/4.3 yet.** Check the `iceberg-spark-runtime-<spark>_<scala>` artifact exists before choosing a Spark version.
3. **`--packages` drags in jars Spark already has** (hadoop-client, slf4j, snappy...). The Dockerfile skips them.
4. **The `spark-sql` shell needs a writable working directory**, because it creates a local Hive metastore (Derby) even though the tables live in Iceberg.
5. **The first snapshot of each table is empty**: creating the table from an empty DataFrame is itself a commit.
6. **`VERSION AS OF` needs a literal**, not a subquery. Look the snapshot id up first.
7. **In `CALL ... rewrite_data_files`, give the fully qualified table name**
   (`lakehouse.bronze.merchants`). With `bronze.merchants`, the procedure first
   tries `bronze` as a catalog and logs a long (harmless) stack trace.
8. **Testing crashes**: the JVM is PID 1 in the container, and the kernel ignores
   SIGKILL sent to PID 1 from inside its own namespace. Kill the Python driver
   instead. And a `grep` for the process name can match the killing shell itself.
9. **Debezium survives a Postgres restart**: it logs a retriable error, waits 10 s, reattaches to the slot.

## Interview questions this step prepares you for

1. How does Spark Structured Streaming achieve exactly-once into a table? What are the three ingredients?
2. What's in a Structured Streaming checkpoint, and what happens if you delete it?
3. How do you consume Confluent Avro in Spark, and why isn't `from_avro` alone enough?
4. A source team adds a column. Walk through what happens in your pipeline.
5. What does Iceberg give you over Parquet files in S3? Explain snapshots and the catalog's role.
6. What's hidden partitioning? Why partition bronze by event time rather than ingestion time?
7. Your streaming job creates thousands of small files. Why is that bad, and what do you do?
8. Trigger interval trade-offs: latency vs file size vs cost. When would you use `availableNow`?
9. What is `failOnDataLoss`, and when would you set it to false?

## Commands

```bash
docker compose up -d --wait                  # whole stack incl. spark-bronze
docker compose logs -f spark-bronze          # job output
open http://localhost:4040                   # Spark UI (streaming tab: batch durations, rates)
open http://localhost:9001                   # object store console

./spark/sql.sh -e "SELECT op, count(*) FROM bronze.payments GROUP BY op"
./spark/sql.sh -e "SELECT * FROM bronze.payments.snapshots ORDER BY committed_at DESC LIMIT 5"
./spark/sql.sh -e "CALL lakehouse.system.rewrite_data_files(table => 'lakehouse.bronze.payments')"
./spark/sql.sh                               # interactive

# One-off catch-up instead of the always-on service
docker compose run --rm --no-deps spark-bronze \
  /opt/spark/bin/spark-submit /opt/lakehouse/jobs/bronze_cdc.py --available-now
```
