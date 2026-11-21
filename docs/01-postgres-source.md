# Phase 1, Step 1: Postgres source database

## What this step builds

A Postgres 17 container that plays the role of a payment gateway's production
database (the "OLTP source"). Everything downstream (Debezium, Kafka, Spark,
Iceberg) reads from it. It is configured for change data capture (CDC) from day one.

| File | Purpose |
|---|---|
| `docker-compose.yml` | Runs Postgres with `wal_level=logical` and a WAL cap per replication slot |
| `postgres/init/01_schema.sql` | Five tables, constraints, indexes, `updated_at` triggers |
| `postgres/init/02_cdc_setup.sh` | `debezium` role (replication + read-only) and the `dbz_payments` publication |
| `.env` / `.env.example` | Passwords. `.env` is gitignored, `.env.example` is the template |

## Data model

```
customers ──< payment_methods          ──<  means "one to many"
    │               │
    └──────< payments >────── merchants
                │
                └──< refunds
```

- **merchants**: `pricing_plan` and `mdr_bps` (fee rate) change over time. This is
  why the merchant dimension will be SCD Type 2 later: a payment's fee must be
  calculated with the rate that was active *when the payment happened*.
- **customers**: `email` and `phone` are PII, to be masked or hashed in the lakehouse.
- **payment_methods**: UPI, card, netbanking, wallet. Card-only fields are enforced by a CHECK.
- **payments**: one row per *attempt*. An order (`merchant_order_id`) can have
  several attempts, which lets us compute both attempt success rate and order
  conversion rate, two different metrics payment companies track.
- **refunds**: partial or full, against a payment.

## Key concepts

**Why CDC instead of `SELECT ... WHERE updated_at > last_run`?**
Polling misses deletes, misses intermediate states (created then captured between
two polls looks like one change), and adds query load on production. CDC reads the
write-ahead log (WAL), so it sees every committed change in commit order with
little extra load on the database.

**WAL and `wal_level=logical`.** Postgres writes every change to the WAL before it
touches the table files, so it can recover after a crash. `logical` adds enough
detail (table names, column values) for an external tool to decode row changes.
It produces somewhat more WAL and needs a restart to change.

**Replication slot.** A server-side bookmark of how far a consumer has read.
Postgres keeps every WAL segment the slot hasn't confirmed. If Debezium stops for
a weekend, WAL piles up until the disk fills and the database goes down.
`max_slot_wal_keep_size=2GB` caps this: past 2 GB the slot is invalidated, which
protects the database but means the connector must re-snapshot. That is a
deliberate trade-off: database availability over pipeline continuity.

**Publication.** The list of tables the `pgoutput` plugin streams. We create it
as the table owner, so the Debezium role only needs `REPLICATION` + `SELECT`
(SELECT is for the initial snapshot). Least privilege.

**What the WAL actually contains.** The verification used Postgres's built-in
`test_decoding` plugin to print decoded changes. One transaction looked like:

```
BEGIN 766
table public.payments: UPDATE: payment_id[bigint]:1 ... status[text]:'captured' ...
table public.refunds: INSERT: refund_id[bigint]:1 payment_id[bigint]:1 amount_paise[bigint]:10000 ...
COMMIT 766
```

Notice: changes come grouped by transaction, and the UPDATE carries only the
*new* row. The old values are not included because tables default to
`REPLICA IDENTITY DEFAULT` (only the primary key identifies the old row).
`REPLICA IDENTITY FULL` would include the full before-image at the cost of more WAL.

## Design choices worth defending

| Choice | Reason |
|---|---|
| Money as `BIGINT` paise | Floats can't represent 0.10 exactly. `NUMERIC` also works, but Debezium sends NUMERIC as raw bytes by default (`decimal.handling.mode`), which is awkward downstream |
| `TIMESTAMPTZ` everywhere | Stored as UTC, no ambiguity across time zones or DST |
| `TEXT + CHECK` over `ENUM` | Adding a status value is a constraint change, not `ALTER TYPE` |
| `UNIQUE (merchant_id, idempotency_key)` | A retried API call (network timeout) can't create a duplicate charge |
| Composite FK `(payment_method_id, customer_id)` | The database itself guarantees a customer can't pay with someone else's card |
| Explicit FK indexes | Postgres does not index foreign keys automatically |

## Gotchas

1. **Init scripts run only on an empty volume.** Editing `01_schema.sql` does
   nothing to a running database. To re-run them: `docker compose down -v`
   (this deletes all data), then `up` again.
2. **`now()` is the transaction start time**, not the wall clock. All rows written
   in one transaction share it (in the demo, `captured_at`, the refund's
   `created_at`, and `updated_at` were identical).
3. **Never leave an unused replication slot behind.** The demo slot was dropped
   right after use. Check with `SELECT * FROM pg_replication_slots;`.
4. **On AWS RDS/Aurora** you can't pass `-c wal_level=logical`. You set
   `rds.logical_replication = 1` in a parameter group and reboot.

## Interview questions this step prepares you for

1. How does Debezium capture changes from Postgres? What has to be configured on the database?
2. What is a replication slot, and what happens if the consumer goes down?
3. Why would you choose log-based CDC over query-based incremental loads?
4. How do you store money in a database, and why?
5. What is an idempotency key, and where does it matter in a payments system?
6. What does `REPLICA IDENTITY` control, and when would you set it to `FULL`?

## Commands

```bash
colima start                                              # start the Docker VM
docker compose up -d --wait                               # start Postgres, wait until healthy
docker compose exec postgres psql -U postgres -d payments # SQL shell
docker compose down                                       # stop, keep data
docker compose down -v                                    # stop and DELETE data (re-runs init scripts next time)
colima stop                                               # free the 8 GB of RAM when done
```
