# Phase 1, Step 2: Data generator

## What this step builds

A Python package (`generator/`) that fills the source database with realistic
payment data, in two modes:

| Command | What it does |
|---|---|
| `python -m generator seed` | Once, on an empty database: 300 merchants, 20,000 customers, ~31,000 payment methods, and 30 days of history (~660K payment attempts, ~20K refunds). Takes about 12 s |
| `python -m generator run` | Live stream: new orders (1/s by default), each moving through its lifecycle in real time, plus occasional reference-data changes. Ctrl+C to stop |
| `python -m generator stats` | Row counts, success rates, refunds, recent activity |

Useful flags: `run --rate 5 --dim-rate 2 --duration 60`, `seed --orders-per-day 5000`.
Run `python -m generator seed --help` for the rest.

| File | Purpose |
|---|---|
| `generator/reference.py` | Cities, categories, payment-method mix, failure rates, and the random helpers |
| `generator/seed.py` | Bulk history load with `COPY` |
| `generator/simulate.py` | Live mode: event scheduler, payment lifecycle, reference-data changes |
| `generator/stats.py` | Summary queries |
| `requirements.txt` | Pinned versions: psycopg 3, Faker, python-dotenv |

## What the data looks like (measured after seeding)

| Measure | Value |
|---|---|
| Attempt success rate | UPI 91.0%, card 87.1%, wallet 95.0%, netbanking 84.2% |
| Order conversion (any attempt succeeded) | 95.4%, 1.06 attempts per order |
| Median ticket | gaming ₹199, food ₹349, ecommerce ₹1,197, travel ₹5,489 |
| Traffic skew | top 10% of merchants get 47% of payments |
| Daily shape | peak at 9 pm IST, trough at 3 am; weekends ~15% busier |

All integrity checks returned 0 violations: refunds never exceed the payment,
only captured payments are refunded, nothing happens before the customer or
method existed, no timestamps in the future, payment_id increases with time.

## How it works

### Seed mode: bulk load
- **`COPY` instead of `INSERT`.** COPY streams all rows in one command, so
  ~710K rows load in about 12 s. Row-by-row INSERTs would need one network
  round trip each.
- **One transaction.** Either the whole seed lands or nothing does, so a crash
  can't leave a half-seeded database.
- **`ANALYZE` afterwards.** Refreshes the query planner's statistics. Without
  it, the planner still thinks the tables are empty and can pick bad plans.
- **Consistent timeline.** All merchants, customers and methods are created
  *before* the history window starts, so no payment predates its customer.
  Rows are inserted in time order, so ids increase with `created_at`.
- **Reproducible.** `--random-seed 42` (the default) gives the same distribution every run.

### Live mode: event scheduler
- **Poisson arrivals.** The gaps between orders follow an exponential
  distribution: random, but averaging `--rate` per second. That's how
  independent customers actually arrive.
- **A priority queue (heap) of future events.** Creating a payment schedules
  "resolve in 1 to 4 s". Resolving schedules "capture in 1 to 4 s" or a retry.
  Capturing may schedule a refund minutes later. One thread and one connection.
- **One transaction per state change.** A payment is INSERTed as `created`, then
  UPDATEd to `authorized`, then to `captured`, seconds apart. **CDC will therefore
  emit 3+ events per payment**, which matters for every later step.
- **Transitions are guarded**: `UPDATE ... WHERE payment_id = %s AND status = 'created'`.
  If the same step ever ran twice, the second would change nothing.
- **Idempotency is exercised.** 0.5% of requests are re-sent with the same
  idempotency key (a merchant's HTTP client timing out and retrying).
  `ON CONFLICT DO NOTHING` absorbs them, and no duplicate charge is created.
- **Recovery on restart.** Stopping mid-stream leaves some payments in `created`
  or `authorized`. The next `run` finds them and finishes them. Scheduled retries
  and refunds that hadn't happened yet are dropped, like a customer who gives up.

### Reference-data changes (live mode)
Customer signups (customer + first method in **one** transaction), KYC decisions,
customers moving city, methods added/removed, **merchant plan changes** (the SCD
Type 2 feed), merchant suspensions/reactivations, merchant signups. Suspended
merchants get no new payments. A removed method is hard-deleted only if it was
never used, otherwise soft-deleted (`is_active = false`) to keep payment history intact.

## Why this matters for later steps

| Generator behaviour | What it forces downstream |
|---|---|
| 3+ change events per payment | The lakehouse must **upsert** (MERGE latest state per key), not just append |
| Historical rows exist before CDC starts | Debezium's **initial snapshot** (`op = r`) vs. streamed changes (`op = c/u/d`) |
| Signup + first payment touch two tables | Events land on different Kafka topics: a payment can arrive before its customer (**late-arriving dimension**) |
| Merchant plan changes | **SCD Type 2**: fees must use the rate active at payment time |
| Hard and soft deletes | Delete events and tombstones in CDC; soft deletes are just updates |
| Top merchants get half the traffic | **Data skew** in Spark joins and aggregations |

## Finding: check the plan before adding an index

While building "remove a payment method", the first guess was that the composite
foreign key `payments (payment_method_id, customer_id)` needed its own index.
Measuring first:

| Query | Plan | Time |
|---|---|---|
| FK check when deleting a method | uses existing `idx_payments_customer` | 0.05 ms |
| Generator's `NOT EXISTS (... WHERE payment_method_id = x)` | **sequential scan of 658K rows** | 19 ms |
| Same, also matching `customer_id` | uses `idx_payments_customer` | 0.058 ms |

The schema was fine. The slow part was the generator's query, so the fix went
into the query, not the schema. An index on `payments`, the busiest table, would slow down
every payment insert to speed up a rare delete. Lesson: run `EXPLAIN ANALYZE`
before you add an index.

## Gotchas

1. **`seed` refuses a non-empty database.** To start over:
   `docker compose down -v && docker compose up -d --wait`, then seed again.
2. **Live volume is higher than history.** History is ~20K orders/day; `run` at
   1 order/s is ~86K/day. Use `--rate 0.25` to match. The higher default is
   so the stream is easy to watch.
3. **The live generator uses an unseeded RNG** on purpose: a seeded one would
   regenerate the same idempotency keys after a restart, and `ON CONFLICT`
   would silently drop those payments.
4. **Emails use reserved domains** (`example.com/.org/.net`, RFC 2606), so fake
   data can never reach a real inbox.
5. The generator connects as the `postgres` superuser. In production an app
   would get its own role with only INSERT/UPDATE/SELECT on these tables.

## Interview questions this step prepares you for

1. How would you bulk-load a million rows into Postgres quickly? Why is COPY faster?
2. What is an idempotency key? Show how a database constraint enforces it.
3. A payment emits created, authorized and captured events. How do you build a
   table of current payment state from that stream?
4. What is a late-arriving dimension, and how would you handle it?
5. Payment success rate vs. order conversion rate: what's the difference, and why track both?
6. When would you add an index, and what does it cost? (Every write maintains it.)
7. Hard delete vs. soft delete: trade-offs, and how each looks in CDC.

## Commands

```bash
source .venv/bin/activate
pip install -r requirements.txt
python -m generator seed                    # once
python -m generator run                     # Ctrl+C to stop
python -m generator run --rate 5 --duration 60
python -m generator stats
```
