# Phase 1, Step 3: Kafka, Schema Registry and Debezium (CDC)

## What this step builds

Every committed change in Postgres now flows into Kafka within about a second,
as an Avro message whose schema is stored in Schema Registry.

```
Postgres WAL ──► replication slot ──► Debezium (inside Kafka Connect) ──► Kafka topics
                 debezium_payments     reads the slot, wraps each row      payments.public.<table>
                                       change in an event envelope         Avro, 3 partitions each
                                                │
                                                └── registers schemas ──► Schema Registry
```

| Service | Image | Host port | Role |
|---|---|---|---|
| kafka | `confluentinc/cp-kafka:8.4.0` | 9092 | Single broker in KRaft mode (no ZooKeeper) |
| schema-registry | `confluentinc/cp-schema-registry:8.4.0` | 8081 | Stores Avro schemas, enforces compatibility |
| connect | built from `connect/Dockerfile` | 8083 | Kafka Connect worker + Debezium 3.7.1 Postgres connector |
| kafka-ui | `kafbat/kafka-ui:v1.5.0` | 8080 | Browse topics, decoded messages, schemas, connectors |

| File | Purpose |
|---|---|
| `connect/Dockerfile` | Adds the Debezium plugin to Confluent's Connect image (downloaded and checksum-verified in a build stage) |
| `connect/payments-postgres-cdc.json` | Connector config. The password is `${env:DEBEZIUM_PASSWORD}`, never the real value |
| `connect/register.sh` | Creates or updates the connector via the Connect REST API |
| `tools/reconcile_cdc.py` | Replays every topic and checks it matches Postgres row for row |

## Measured results

| Check | Result |
|---|---|
| Initial snapshot | 730,399 rows from 5 tables in **6 seconds**; per-table counts identical to Postgres |
| Messages per topic after snapshot | equal to table row counts (e.g. payments: 658,962 = 658,962) |
| Live streaming | each payment produced `c`, `u`, `u` events from 3 separate transactions |
| **Crash test**: `docker compose kill connect` (SIGKILL) while the generator kept writing, down 25 s | slot went inactive and Postgres held 2.7 MB of WAL; on restart Debezium found its offset, skipped the snapshot, caught up to 56 bytes behind |
| Reconciliation after the crash | **PASS**: 0 missing, 0 extra, 0 stale in all 5 tables; 51 events delivered twice |

## Key concepts

**Snapshot, then streaming, with no gap.** On first start Debezium creates the
replication slot, and Postgres hands back a snapshot that matches the slot's
starting LSN exactly. Debezium reads every table inside that snapshot (events
with `op = r`), then streams from that LSN. A change committed during the
snapshot is never missed and never read twice. The log shows the 7 steps:
`Snapshot step 1 - Preparing` ... `Snapshot step 7 - Snapshotting data`, `Starting streaming`.

**The event envelope.** Every message value has:

| Field | Meaning |
|---|---|
| `op` | `r` snapshot read, `c` insert, `u` update, `d` delete |
| `before` / `after` | row before / after the change (`after` is null for deletes) |
| `source` | where it came from: `lsn`, `txId`, table, `snapshot` flag, commit time |
| `ts_ms` | when Debezium processed it |

**Keys decide ordering.** The message key is the primary key (`{"payment_id": 658970}`).
Kafka sends the same key to the same partition, and a partition is strictly
ordered, so every event for one payment arrives in commit order. There is no
ordering *across* partitions or topics.

**Debezium's position lives in two places.**
1. Kafka Connect's `_connect-offsets` topic: the LSN of the last event sent. Committed every 10 s here (`CONNECT_OFFSET_FLUSH_INTERVAL_MS`).
2. The Postgres slot's `confirmed_flush_lsn`: Debezium confirms a position only after the offset is committed, and only then can Postgres recycle that WAL.

**At-least-once delivery.** After a crash, Debezium restarts from the last
*committed* offset, so events sent after that commit are sent again. That's the
51 duplicates in the crash test. Every consumer must be idempotent: deduplicate on
`(primary key, source.lsn)` or upsert by key. (Kafka Connect also supports
exactly-once for source connectors, at the cost of transactional producers.)

**Avro + Schema Registry.** A message isn't plain Avro. It's 1 magic byte (0),
a 4-byte schema id, then the Avro body. Consumers fetch the schema by id
(and cache it). This matters in step 4: Spark's `from_avro` doesn't understand
the 5-byte header by itself. Compatibility is `BACKWARD`: a new schema version
must be able to read data written with the previous one, so adding a nullable
column is allowed and removing a required one is rejected.

**Two listeners.** Containers reach Kafka at `kafka:29092`, the Mac at
`localhost:9092`. A client first asks any broker for metadata, then connects to
the *advertised* address it gets back, so each audience needs an address it can resolve.

## Design choices

| Choice | Reason |
|---|---|
| Keep the full Debezium envelope (no `ExtractNewRecordState` unwrap) | The raw layer keeps `op`, `before`, LSN and txId: everything needed for dedup, ordering and replays. Flattening happens in Spark |
| `pgoutput` plugin, publication created in step 1, `autocreate.mode=disabled` | Built into Postgres (nothing to install), and Debezium needs no table-owner rights |
| `auto.create.topics.enable=false` | Topics exist only because a connector asked for them, never from a typo |
| 3 partitions per topic | Lets Spark read in parallel; per-key order still holds |
| `retention.ms=-1` | Dev only: Kafka keeps the snapshot until Spark reads it in step 4. Production would keep ~7 days, with the lakehouse as the long-term copy |
| `heartbeat.interval.ms=10000` | Debezium confirms its position even when captured tables are quiet, so the slot doesn't hold WAL needlessly |
| Password through `EnvVarConfigProvider` | The connector config, and the REST API's `GET /config`, show `${env:...}`, not the secret |

## Gotchas found in this step

1. **Updates arrive with `before = null`.** With `REPLICA IDENTITY DEFAULT`, Postgres
   logs only the new row. To see what changed, downstream has to keep the previous state.
2. **Delete events look like they have a full `before`, but don't.** Only the primary
   key is real. Other non-nullable fields are filled with defaults (`customer_id=0`,
   `provider=''`). Never read anything but the key from a delete.
3. **A delete is two messages:** the `d` event, then a tombstone (null value) so
   log compaction can eventually drop the key.
4. **`NotLeaderOrFollowerException` warnings** right after the snapshot starts:
   the producer wrote to a topic created milliseconds earlier and retried.
   Harmless, and the reconciliation proved no loss.
5. **`localhost` resolves to IPv6 first** on macOS, but Docker publishes ports on
   IPv4 only. The Python client logged a connection error before falling back;
   `broker.address.family=v4` fixes it.
6. **`consume(num_messages=N, timeout=T)` waits for N messages *or* T seconds.**
   With a 10 s timeout, a 305-message topic always took 10 s. A short timeout fixed it.
7. **Deleting the connector does not delete the replication slot.** The slot then
   holds WAL forever (up to the 2 GB cap). Clean up with
   `SELECT pg_drop_replication_slot('debezium_payments');`.
8. **Reset Kafka and Postgres together** (`docker compose down -v`). Resetting
   only one leaves offsets and the slot out of sync.

## Interview questions this step prepares you for

1. Walk through how Debezium gets data out of Postgres, from WAL to Kafka topic.
2. How does Debezium avoid gaps or overlaps between the initial snapshot and streaming?
3. Where does a CDC connector store its position? What happens after a crash?
4. Is Debezium exactly-once? How do you make the pipeline correct anyway?
5. Why use Avro and a schema registry instead of JSON? What does BACKWARD compatibility allow?
6. How is ordering guaranteed in Kafka, and what is it *not* guaranteed across?
7. What are advertised listeners, and why do Docker setups need two?
8. A CDC connector was down for a weekend. What happens on the database, and how do you protect it?
9. How did you verify the pipeline didn't lose data? (the reconciliation tool and the crash test)

## Commands

```bash
docker compose up -d --wait               # everything, including the Connect image build
./connect/register.sh                     # create/update the connector (safe to re-run)
curl -s localhost:8083/connectors/payments-postgres-cdc/status
python -m tools.reconcile_cdc             # with the generator stopped
open http://localhost:8080                # Kafka UI

# Debezium's position on the Postgres side
docker compose exec postgres psql -U postgres -d payments -c \
  "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag FROM pg_replication_slots"
```
