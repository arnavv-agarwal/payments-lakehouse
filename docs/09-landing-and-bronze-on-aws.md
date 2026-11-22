# Phase 2, Step 9: Landing batches and bronze on AWS

## What this step builds

Ingestion stays on the laptop (Postgres → Debezium → Kafka → bronze). This
step hands bronze over to AWS in **landing batches**: the new bronze rows are
exported to Parquet, uploaded to S3, and appended to Iceberg bronze tables in
the Glue Data Catalog by the first **AWS Glue** job.

```
local bronze (Iceberg, RustFS)
   │  spark/jobs/export_bronze.py      new rows since the last export → Parquet + manifest
   ▼
landing/bronze/<table>/batch_id=<id>/*.parquet      (on the laptop, gitignored)
landing/bronze/_manifests/<id>.json
   │  tools/push_landing.sh            aws s3 sync, with your CLI login
   ▼
s3://<bucket>/landing/bronze/...
   │  Glue job payments-lakehouse-load-bronze (spark/jobs/load_bronze.py)
   ▼
lakehouse.bronze.* on AWS (Iceberg v2, Glue Data Catalog) + ops.bronze_loads
```

| File | Purpose |
|---|---|
| `spark/jobs/export_bronze.py` | runs locally (`spark-tools`); writes one landing batch and its manifest; bookmark in `landing/_state/export_state.json` |
| `tools/push_landing.sh` | uploads `landing/bronze` to S3 (`aws s3 sync`, skips Spark's `.crc`/`_SUCCESS` files) |
| `spark/jobs/load_bronze.py` | runs on Glue; verifies the batch against its manifest and appends rows bronze doesn't have |
| `infra/glue_jobs.tf` | the Glue job, its script upload, and Glue's log groups (14-day retention) |
| `tools/glue_run.sh` | starts a Glue run, waits, prints duration / DPU-seconds and the job's output |
| `tools/athena_query.sh` | runs one SQL statement in the Athena workgroup and prints rows + data scanned |

## Decisions

**Landing zone instead of writing to AWS from the laptop's Spark.** Local
Spark could write straight into the AWS tables, but then AWS credentials would
have to go into the containers. With a landing zone, the only thing touching
AWS from the laptop is `aws s3 sync` under the CLI login, and the AWS side owns
its tables. It's also the common real-world shape: files land in a raw area,
and a job loads them into managed tables.

**Batches are defined by Kafka coordinates.** Every bronze row has a unique
`(kafka_partition, kafka_offset)`, and offsets only grow within a partition.
The export keeps the highest exported offset per topic partition and takes
everything above it next time. That bookmark is written **last**, after the
Parquet files and the manifest: a crash mid-export means the next export
repeats some rows, never skips any.

**The manifest is the contract.** It lists, per table, the row count and the
offset range per partition. The load job refuses a batch whose files don't
contain exactly the manifest's row count (a partial upload fails loudly).

**Idempotent load by anti-join.** `load_bronze.py` appends only rows whose
Kafka coordinates aren't already in the table. Re-running a batch, or loading
overlapping batches after an export retry, adds nothing. An empty result
writes no commit at all.

**Lineage.** Each append commit carries `landing-batch-id` in its Iceberg
snapshot summary, and `ops.bronze_loads` records rows-in-batch and
rows-appended per table and run.

**Same layout as local bronze.** Partitioned by `days(kafka_timestamp)`,
`write.spark.accept-any-schema` with `mergeSchema` on append, so a new source
column flows through as it does locally. Format version 2 so Athena can read it.

**Same job code locally and on Glue.** The Glue job's Spark settings (in
`infra/glue_jobs.tf`) mirror `spark-defaults.conf`, with the catalog named
`lakehouse` but of `type=glue`. Table names are identical. The one difference
in code: the JDBC catalog password is set only when the environment provides
it (locally); on Glue, the job's IAM role grants catalog access.

**Glue job settings.** Glue 6.0, 2 × G.1X workers (the minimum), STANDARD
execution class, 15-minute timeout, no Glue-side retries (the orchestrator
decides), one concurrent run (the same single-writer rule as locally).

## Measured results

| Run | Result |
|---|---|
| Export, batch 1 (full history) | 748,899 rows, 5 tables, 10 s; 86 MB of Parquet |
| Export again immediately | "Nothing new to export" |
| Local test load into a scratch namespace | exact copy (`EXCEPT ALL` both directions: 0 rows); second load appended 0 |
| Upload batch 1 | 6 files, 86 MB, 16 s |
| **Glue load, batch 1** | SUCCEEDED in 118 s, 237 DPU-seconds; 748,899 rows appended |
| Local vs AWS (Spark vs Athena) | identical per table: row count, distinct Kafka keys, offset sum, deletes, first/last timestamp; payments amount sum 142,117,074,400 paise and 600,914 captured events on both |
| Live traffic (2 min at 3 orders/s), export batch 2 | 1,079 new rows only |
| **Glue load, batch 2** | SUCCEEDED in 93 s; 1,079 appended |
| **Glue load, batch 2 again** | SUCCEEDED in 92 s; 0 appended, 1,079 skipped, no new snapshot |
| Local vs AWS after batch 2 | identical (676,123 payments events; all 5 tables) |

Cost of the three Glue runs: 607 DPU-seconds ≈ 0.17 DPU-hours, a few US
cents. Athena scanned about 1 MB per verification query.

## Gotchas

1. **Column order after a join.** `df.join(other, ["kafka_partition", "kafka_offset"], "left_anti")`
   returns the join columns first. Appending that to the Iceberg table failed
   with `Cannot write incompatible dataset to table with schema`: with
   `accept-any-schema`, the write matched columns by position. Fixed with
   `.select(*src.columns)` after the join.
2. **`ConcurrentRunsExceededException` right after a run succeeded.** With
   `max_concurrent_runs = 1`, Glue keeps the slot busy for several seconds
   after the run reports SUCCEEDED. `glue_run.sh` retries the start for up to 2 minutes.
3. **The first snapshot of each AWS table is an empty append.** The table is
   created with `CREATE TABLE ... AS SELECT ... WHERE false` (to copy the
   batch's schema), then the rows are appended in a second commit that carries
   the batch id.
4. **Spark writes `.crc` and `_SUCCESS` files on a local filesystem.** They
   aren't uploaded; the manifest is the completeness record.
5. **Generator stopped with payments in flight.** Their later state changes
   arrive the next time the generator runs (it resumes in-flight payments),
   so they'll be in a later batch.

## Commands

```bash
# 1. export new bronze rows (local)
docker compose run --rm spark-tools /opt/spark/bin/spark-submit /opt/lakehouse/jobs/export_bronze.py
# 2. upload
./tools/push_landing.sh
# 3. load on Glue (batch id = manifest name)
./tools/glue_run.sh payments-lakehouse-load-bronze --batch-id <id>
# 4. look at it with Athena
./tools/athena_query.sh "SELECT op, count(*) FROM bronze.payments GROUP BY op"
./tools/athena_query.sh "SELECT * FROM ops.bronze_loads ORDER BY loaded_at"
```

## Interview questions this step prepares you for

- How do you move data from an on-premise system to the cloud incrementally, without duplicates and without gaps?
- Why write the bookmark last? What happens if the job crashes at each point?
- What's a manifest file for?
- How would you trace a row in the lake back to the batch (and file) that delivered it?
- Glue job vs a Spark cluster you run yourself: what do you give up and gain? What's a DPU?
