# Phase 1, Step 6: Gold star schema with an SCD Type 2 merchant dimension

## What this step builds

A batch job (`spark/jobs/gold_star.py`) that turns silver's current-state
tables into a Kimball star schema for analytics.

```
                     dim_date (calendar, Indian fiscal year)
                          │ date_key
 dim_merchant (SCD2) ── fact_payments ── dim_customer (SCD1)
   merchant_sk            │      │
                          │      └── dim_payment_method (SCD1)
                     fact_refunds (shares the same dimensions)
```

| Table | Grain (one row per...) | Built |
|---|---|---|
| `fact_payments` | payment attempt | incremental MERGE |
| `fact_refunds` | refund | rebuilt each run |
| `dim_merchant` | merchant **version** (SCD Type 2) | rebuilt each run from bronze history |
| `dim_customer` | customer (SCD Type 1, no PII) | rebuilt each run |
| `dim_payment_method` | payment method (SCD Type 1) | rebuilt each run |
| `dim_date` | calendar day, 2024 to 2027 | rebuilt each run |
| `pipeline_state` | one row: the watermarks of the last run | replaced each run |

`fact_payments` measures: `amount_paise`, `fee_paise` (at the rate valid at
payment time), `net_settlement_paise`, `refunded_paise`, `refund_count`,
`is_captured` / `is_failed` (summable 0/1), `seconds_to_authorize`,
`seconds_to_capture`, plus `attempt_number` within the order.

## Measured results

| Check | Result |
|---|---|
| First (full) build | 12 s: 473 merchant versions (352 merchants + 121 past versions), 661,704 facts |
| SCD2 invariants: one current version per merchant, no gaps, no overlaps, version 1 starts at `created_at`, current version = source | **0 violations** each |
| Every payment matches exactly one merchant version | **0 violations** (661,704 payments) |
| Fee arithmetic, fact totals vs silver | 0 differences |
| Example: merchant 230, 4 versions (2.00% → 1.80% → 1.47% → 1.69%) | each payment charged the rate of its version |
| **Late-arriving merchant**: payment in gold before its merchant | inferred member, fee NULL; next run: real version, payment re-keyed, fee ₹13.50 |
| **Incremental = full rebuild** after live traffic (gold run twice mid-stream) | **PASS**: 0 rows differ |
| Why SCD2 matters: fees of merchants that changed plan, by plan | correct (plan at payment time): enterprise ₹1.57 lakh; naive (current plan): ₹11.0 lakh, 7× too high |

## Key concepts

**Grain first.** Every fact table is defined by what one row means. `fact_payments`
is one row per *attempt*, so success rate = `avg(is_captured)`, and order
conversion needs `attempt_number` or a distinct count of orders.

**Two kinds of fact table.**
- `fact_refunds` is a **transaction fact**: a row is written once and never changes.
- `fact_payments` is an **accumulating snapshot**: one row per payment, updated as it
  reaches each milestone (authorized, captured) and as refunds land, with lag
  measures between milestones.

**SCD types.** Type 1 overwrites (customers: current city, KYC status). Type 2
keeps history with a new row per change, each with `valid_from`, `valid_to` and
`is_current`. `dim_merchant` is a **hybrid**: plan, fee rate, status, category
and location are Type 2. Name and GSTIN are Type 1 (always the current value):
you want today's legal name even on last year's invoices.

**SCD2 from a CDC log.** Bronze holds every merchant change in WAL order, so
versions are a pure function of that history: keep the events that changed a
Type 2 attribute (compare with the previous event via `lag`). Each one starts a
version, and the next one's start ends it (`lead`). Because it's deterministic,
it's rebuilt from scratch every run, which is cheap for a 474-row dimension.

**Half-open intervals.** A version covers `[valid_from, valid_to)`. With
`BETWEEN`, a payment at exactly the change moment would match two versions and
be double-counted. The check "every payment matches exactly one version" guards this.

**Point-in-time join.** `fact.merchant_id = dim.merchant_id AND fact.created_at
>= dim.valid_from AND fact.created_at < dim.valid_to`. That's how a payment
gets the fee rate that applied when it happened.

**Deterministic surrogate keys.** `merchant_sk = xxhash64(merchant_id, valid_from)`,
`customer_sk = xxhash64(customer_id)`. A rebuild gives every row the same key, so
facts never need re-keying after a dimension rebuild. (64-bit hash collisions at
this scale are vanishingly unlikely; a uniqueness check would catch one.)

**Late-arriving dimensions.** Kafka topics and streaming queries run
independently, so a payment can reach gold before its merchant. Gold doesn't
drop the payment or guess: it creates an **inferred member** ("Unknown (not yet
loaded)") and leaves the fee NULL, *unknown*, not *wrong*. When the merchant
arrives, its real version is new, so the payments from its `valid_from` onward
are recomputed and re-keyed. The same mechanism fixes a merchant *change* that
arrives late.

**Incremental fact loads.** Each run recomputes only:
1. payments whose silver row changed since the last run (`_silver_updated_at` > watermark),
2. payments whose refunds changed,
3. payments affected by new merchant versions,
4. every attempt of those payments' orders (attempt numbers depend on each other),

and then MERGEs them in. The watermark is the newest change **in the data read**,
never the clock, and it's saved last. A run that dies midway is simply
redone, because the MERGE is idempotent. That happened once while building this:
a crash after the dimensions were written but before the watermark was saved;
the next run recomputed the right payment.

**`--check`: incremental must equal full.** The classic risk of incremental
pipelines is drift. The job can recompute the whole fact table in memory and
diff it against the table (`exceptAll` both ways). PASS after live traffic.

**Money and time.** Fees stay in integer paise: `(amount × bps + 5000) div 10000`
rounds half up with no floating point. The business date is the **IST** date
(`date_key`), not the UTC date: a payment at 00:30 IST belongs to that IST day.
`dim_date` carries India's April–March fiscal year (`FY2026-27`, Q1 = Apr–Jun).

## Design choices

| Choice | Reason |
|---|---|
| Rebuild small tables, MERGE the big one | Dimensions (~52K rows) rebuild in seconds and can't drift; the 661K-row fact is incremental, as it would be at scale |
| `dim_merchant` built only from bronze history | One source and one clock. A first version took name/GSTIN from silver, and a lagging silver gave a real version with a NULL name |
| Version 1 starts at the merchant's `created_at` | History before CDC started is unknowable; stated as an assumption, not hidden |
| Inferred fee = NULL, not 0 or a default rate | Revenue reports must not silently include a guess |
| Gold tables copy-on-write (silver was merge-on-read) | Gold is read by BI far more than it's written; silver is written every minute |
| Inputs pinned to one snapshot per table for the whole run | Streaming jobs keep committing; a run must not see two versions of a table |
| Customers Type 1 | Simpler and privacy-friendly; a "customer city at payment time" requirement would make it Type 2 |

## Gotchas found in this step

1. **Spark 4.1 + Iceberg MERGE**: a DataFrame from `spark.table()` carries a lazy
   `TableReference` that Iceberg's MERGE can't plan inside the MERGE source
   (`No plan for TableReference`). Reading with `SELECT ... VERSION AS OF` fixed it.
2. **"Most recent snapshot" is not "current snapshot"** after a rollback: use the
   `main` entry in the `refs` metadata table. (Fixed in silver too.)
3. **Mixing layers inside one dimension** gave a version with a NULL name (see design choices).
4. **A CTE belongs to one statement.** The first version of the point-in-time demo
   query failed because its `WITH` was attached to a different statement.
5. **Test data left in the source**: two merchants named "Late Arrival Test ..."
   and their two payments exist in Postgres, from the late-arriving test.

## Interview questions this step prepares you for

1. What is the grain of your fact table? Why does it matter for metrics like success rate?
2. Transaction vs periodic vs accumulating snapshot facts: which is `fact_payments`?
3. Explain SCD Types 1, 2 and a hybrid. Which attributes would you track, and why?
4. How do you implement SCD2? How did you build it from a CDC stream?
5. Why half-open intervals? What breaks with BETWEEN?
6. How do you join a fact to the dimension version valid at event time?
7. Natural vs surrogate keys; sequence vs hash surrogate keys: trade-offs?
8. What's a late-arriving dimension? What's an inferred member, and how is it fixed later?
9. How do you build a fact table incrementally without missing or double-counting changes? How do you prove the incremental logic is right?
10. Why do analysts get wrong revenue numbers without SCD2? (The 7× example.)

## Commands

```bash
# Build or update gold (incremental; the first run is a full refresh)
docker compose run --rm spark-tools /opt/spark/bin/spark-submit /opt/lakehouse/jobs/gold_star.py
# Rebuild fact_payments from scratch / verify incremental == full (writes nothing)
docker compose run --rm spark-tools /opt/spark/bin/spark-submit /opt/lakehouse/jobs/gold_star.py --full-refresh
docker compose run --rm spark-tools /opt/spark/bin/spark-submit /opt/lakehouse/jobs/gold_star.py --check

./spark/sql.sh -e "SELECT * FROM gold.dim_merchant WHERE merchant_id = 230 ORDER BY version_number"
./spark/sql.sh -e "
  SELECT d.category, round(100 * avg(f.is_captured), 1) AS success_pct, sum(f.fee_paise) / 100 AS fees_inr
  FROM gold.fact_payments f JOIN gold.dim_merchant d USING (merchant_sk) GROUP BY d.category"
```
