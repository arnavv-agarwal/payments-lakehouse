# Phase 1, Step 5: Silver, the current state of every row

## What this step builds

A second streaming job that turns bronze's history of change events into
**one current row per primary key**, typed and with personal data removed.

```
bronze.<table>  every change event        silver.<table>  current state
  payment 658970: c  status=created  ─┐
  payment 658970: u  status=authorized ├─►  payment 658970  status=captured  _lsn=500363464
  payment 658970: u  status=captured  ─┘
```

| File / service | Purpose |
|---|---|
| `spark/jobs/silver_cdc.py` | Streaming job: reads new bronze snapshots, MERGEs into silver every 60 s |
| `spark/jobs/reconcile_silver.py` | Compares every silver row and column with Postgres |
| `spark-silver` service | Runs the silver job continuously (Spark UI on 4041) |
| `spark-tools` service | On-demand Spark for SQL and checks (`docker compose run`, never started by `up`) |

Every silver table has the source columns plus:

| Column | Meaning |
|---|---|
| `_op` | last operation applied (`r`, `c`, `u`, `d`) |
| `_lsn` | Postgres WAL position of that change: the ordering key |
| `_source_ts` | when the change was committed in Postgres |
| `_is_deleted`, `_deleted_at` | soft delete: the row was deleted in the source |
| `_silver_updated_at` | when silver last changed the row |

`silver.customers` has **no name, email or phone**: it has `email_hash`
(HMAC-SHA256 with a secret key) and `phone_last4` instead.

## Measured results

| Test | Result |
|---|---|
| Initial build from bronze (737K events) | 12.6 s |
| **Reconciliation** against Postgres | **PASS**: 732,922 rows, every column equal (e.g. 14 columns × 661,010 payments) |
| Negative test: corrupt one silver row on purpose | reconciliation **FAIL**, 1 row, columns `status, failure_reason`; fixed with `rollback_to_snapshot` |
| **Replay**: delete silver's checkpoints, reprocess all of bronze | **+0 rows** in every table, reconciliation still PASS |
| Live: generator + bronze + silver streaming for 3 min | PASS, 0 restarts |
| Latency, Postgres commit → silver | p50 **59.5 s**, p95 88.4 s (Debezium 0.5 s; the rest is the 30 s + 60 s triggers) |

## Key concepts

**Order by LSN, not by time or offset.** Each event's `source.lsn` is its
position in the Postgres write-ahead log. For one row, a later change always has
a higher LSN, because row locks serialise writers. `updated_at` is set by a
trigger at transaction *start*, so it can tie or be out of order. Kafka offsets
only order events within one partition, and a replay or a re-snapshot gives the
same change a new offset.

**Deduplicate before MERGE, or MERGE fails.** A batch can hold several events for
one key (created, authorized, captured), plus Debezium's re-delivered duplicates.
If two source rows match one target row, Spark raises a cardinality error and
refuses to guess. So each batch is first reduced with a window
(`row_number() OVER (PARTITION BY key ORDER BY lsn DESC)`).

**The idempotent MERGE.**
```sql
MERGE INTO silver.payments t USING batch s ON t.payment_id = s.payment_id
WHEN MATCHED AND s._lsn > t._lsn AND NOT s._has_image THEN UPDATE SET <only the delete flags>
WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET <every column>
WHEN NOT MATCHED AND s._has_image THEN INSERT ...
```
`s._lsn > t._lsn` is the whole trick: an event already applied (equal LSN) or
older than the current row is ignored. That's why the full replay changed
nothing. And it's why `foreachBatch`, which on its own is only at-least-once,
becomes effectively exactly-once.

**Deletes without trusting the delete event.** A Debezium delete's `before` has
only the real primary key; the other columns hold placeholder defaults (step 3).
So a delete only sets `_is_deleted`, `_deleted_at` and `_lsn`, never the columns.
If a batch contains a change *and then* a delete for the same key, the row is
written from the last real image and then flagged deleted, so the result
doesn't depend on how events happened to be split into batches.

**Soft deletes.** Deleted rows stay in silver, flagged. Facts that referenced them
still join, and you can answer "when was this removed?". Consumers filter
`NOT _is_deleted`.

**Merge-on-read.** With `write.merge.mode = merge-on-read`, an update writes
the new row plus a small *position delete* file ("row 812 of file X is
replaced"), instead of rewriting the whole data file (copy-on-write). Writes are
cheap, which suits frequent streaming MERGEs. Reads must apply the deletes, so
delete files must be compacted periodically. Copy-on-write is the opposite
trade-off: slow writes, fast reads.

**PII: keyed hash, not plain hash.** `sha256(email)` can be reversed by hashing
a list of known emails. HMAC with a secret key can't, unless the key leaks. The
hash runs in a Python UDF rather than `sha2(concat(key, email))`, because SQL
literals appear in the query plan, which the Spark UI and logs show. The hash
is stable, so silver can still count distinct customers and join on it, and the
step 6 dimension can use it.

**Where latency comes from.** Debezium takes 0.5 s. The rest is waiting for
micro-batch triggers (on average half a trigger at each stage). Shorter triggers
cut latency but create more small files and more commits.

## Design choices

| Choice | Reason |
|---|---|
| Read bronze as an Iceberg stream | Only new snapshots are read each batch; compaction snapshots (`replace`) are skipped automatically, as shown by silver reading the bronze tables compacted in step 4 |
| One query + checkpoint per table | Independent progress and recovery, as in bronze |
| `payments`, `refunds` partitioned by `days(created_at)` | The common query filter; the dimensions are small, so they're unpartitioned |
| Stop when bronze's schema changes | A running stream can't see new columns; the restart adds them to silver (`ALTER TABLE ADD COLUMN`) |
| Iceberg format v2, not v3 | Athena (Phase 2) reads v2; v3's deletion vectors can come later |
| Reconciliation reads Postgres with 4 parallel JDBC queries split on the primary key | Shows how Spark parallelises a JDBC read: `partitionColumn`, bounds, `numPartitions` |

## Gotchas

1. **The Debezium delete image again.** Copying `before.*` into silver would have
   overwritten real values with `customer_id = 0` and empty strings.
2. **A MERGE that changes nothing still commits a snapshot** (the replay produced
   one `+0 rows` snapshot per table). Harmless, but it adds metadata on every
   empty batch, so the job skips the MERGE when a batch is empty.
3. **Bronze still holds raw PII** (the full Debezium envelope). In production,
   bronze access would be restricted (column-level permissions, e.g. Lake
   Formation in Phase 2) and given a short retention; analysts would use silver.
4. **Position deletes accumulate.** Each batch that updates older rows adds delete
   files. Compact with `rewrite_data_files` and `rewrite_position_delete_files`.
5. **Silver doesn't enforce relationships between tables.** Each table runs on
   its own clock, so a payment can briefly exist before its new customer reaches
   silver. That's the late-arriving dimension problem, handled in the gold layer (step 6).
6. **Reconciliation needs a quiet system.** If the generator is writing,
   Postgres is ahead of silver by up to ~90 s and recent rows show as mismatches.

## Interview questions this step prepares you for

1. How do you turn a CDC stream into a current-state table? Why MERGE and not append?
2. Why order by LSN instead of `updated_at` or Kafka offset?
3. How do you make a MERGE idempotent? What happens if a batch is processed twice?
4. What's a MERGE cardinality violation, and how do you avoid it?
5. Hard vs soft deletes in a lakehouse: trade-offs?
6. Copy-on-write vs merge-on-read: when would you choose each?
7. How do you handle PII in a data lake? Why is a plain hash of an email not anonymisation?
8. How do you prove the pipeline is correct? (row-and-column reconciliation, the negative test, the replay test)
9. Your end-to-end latency is 60 s. Where does it come from, and how would you cut it?

## Commands

```bash
docker compose up -d --wait                     # starts spark-silver along with everything else
docker compose logs -f spark-silver             # one line per table per batch
open http://localhost:4041                      # Spark UI for silver

# Reconcile silver against Postgres (stop the generator first)
docker compose run --rm spark-tools /opt/spark/bin/spark-submit \
  --py-files /opt/lakehouse/jobs/silver_cdc.py /opt/lakehouse/jobs/reconcile_silver.py

# Process only some tables (added in step 6), e.g. with the service stopped
docker compose run --rm spark-tools /opt/spark/bin/spark-submit \
  /opt/lakehouse/jobs/silver_cdc.py --available-now --tables merchants

./spark/sql.sh -e "SELECT status, count(*) FROM silver.payments GROUP BY status"
./spark/sql.sh -e "SELECT * FROM silver.payment_methods WHERE _is_deleted"
./spark/sql.sh -e "CALL lakehouse.system.rewrite_position_delete_files('lakehouse.silver.payments')"
```
