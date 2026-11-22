"""Build the gold star schema from silver, plus bronze for merchant history.

    facts       fact_payments       one row per payment attempt. An accumulating
                                    snapshot: the row is updated as the payment moves
                                    created -> authorized -> captured and as refunds land.
                fact_refunds        one row per refund
    dimensions  dim_merchant        SCD Type 2: a new version whenever the plan, fee
                                    rate, status, category or location changes
                dim_customer        SCD Type 1 (current values; PII already removed in silver)
                dim_payment_method  SCD Type 1
                dim_date            calendar with India's April-March fiscal year

Each payment joins the merchant version that was valid when the payment was
made (a point-in-time join), so its fee uses the rate in force at that moment.

Dimensions and fact_refunds are small and rebuilt on every run, atomically.
fact_payments is incremental: only payments that changed since the last run,
plus those affected by new merchant versions, are recomputed and MERGEd.

    spark-submit gold_star.py                 # incremental (full refresh on the first run)
    spark-submit gold_star.py --full-refresh  # rebuild fact_payments from scratch
    spark-submit gold_star.py --check         # compare fact_payments with a full recomputation
"""
import argparse
from datetime import date

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import partitioning

import lakehouse_env

SILVER, BRONZE, GOLD = "lakehouse.silver", "lakehouse.bronze", "lakehouse.gold"
IST = "Asia/Kolkata"
FAR_PAST, OPEN_END = "1900-01-01 00:00:00", "9999-12-31 00:00:00"
CALENDAR = (date(2024, 1, 1), date(2027, 12, 31))
# A change to any of these starts a new merchant version (Type 2).
MERCHANT_TYPE2 = ["category", "city", "state", "pricing_plan", "mdr_bps", "status"]
# These are always shown with their current value on every version (Type 1).
MERCHANT_TYPE1 = ["merchant_name", "gstin"]
UNKNOWN = "Unknown (not yet loaded)"

COMMON_PROPERTIES = {"format-version": "2"}
# Gold is read far more than it is written (BI queries vs a batch every few
# minutes), so updates rewrite data files (copy-on-write) and reads stay simple.
FACT_PROPERTIES = {**COMMON_PROPERTIES, "write.merge.mode": "copy-on-write",
                   "write.update.mode": "copy-on-write", "write.delete.mode": "copy-on-write"}


# ----------------------------------------------------------------------------- inputs

def current_snapshot_id(spark, table):
    if not spark.catalog.tableExists(table):
        return None
    row = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").first()
    return row and row["snapshot_id"]


def pinned(spark, table, snapshot_id=None):
    """Read a table as of one snapshot (by default its current one), so every
    action in this run sees the same data even while streaming jobs commit."""
    return spark.sql(f"SELECT * FROM {table} VERSION AS OF {snapshot_id or current_snapshot_id(spark, table)}")


INPUT_TABLES = {
    "bronze_merchants": f"{BRONZE}.merchants",
    **{t: f"{SILVER}.{t}" for t in ["customers", "payment_methods", "payments", "refunds"]},
}


def inputs(spark, snapshot_ids=None):
    """Input DataFrames pinned to snapshots, plus the snapshot ids used.

    The ids are saved with each run so --check can recompute from exactly the
    data that run saw, even after the streaming jobs have moved on.
    """
    ids = dict(snapshot_ids) if snapshot_ids else {k: current_snapshot_id(spark, t)
                                                   for k, t in INPUT_TABLES.items()}
    return {k: pinned(spark, t, ids[k]) for k, t in INPUT_TABLES.items()}, ids


# ----------------------------------------------------------------------------- dimensions

def ist_date_key(ts):
    """yyyymmdd of the timestamp in India time: the business day, not the UTC day."""
    return F.date_format(F.from_utc_timestamp(ts, IST), "yyyyMMdd").cast("int")


def build_dim_date(spark):
    start, end = CALENDAR
    d = spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS date")
    fy_start = F.when(F.month("date") >= 4, F.year("date")).otherwise(F.year("date") - 1)
    return d.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day_of_month"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        # India's financial year runs April to March: 2026-11-21 is in FY2026-27, Q3.
        F.concat(F.lit("FY"), fy_start, F.lit("-"), F.lpad(((fy_start + 1) % 100).cast("string"), 2, "0"))
         .alias("fiscal_year"),
        # Apr-Jun = Q1 ... Jan-Mar = Q4: shift months so April becomes 0, then 3 per quarter.
        (F.floor(((F.month("date") + 8) % 12) / 3) + 1).cast("int").alias("fiscal_quarter"),
    )


def build_dim_merchant(src):
    """SCD Type 2 from the full change history in bronze, ordered by WAL position."""
    events = (src["bronze_merchants"].where("op IN ('r', 'c', 'u')")
              .select(F.col("after.merchant_id").alias("merchant_id"),
                      F.col("source.lsn").alias("lsn"),
                      F.timestamp_millis("source.ts_ms").alias("committed_at"),
                      F.col("after.created_at").cast("timestamp").alias("created_at"),
                      *[F.col(f"after.{c}").alias(c) for c in MERCHANT_TYPE2 + MERCHANT_TYPE1])
              .dropDuplicates(["merchant_id", "lsn"]))  # Debezium re-deliveries
    by_merchant = Window.partitionBy("merchant_id").orderBy("lsn")
    # Type 1 values: each merchant's newest event. Taken from the same bronze
    # history as the versions, so the dimension never mixes two layers that
    # run on different clocks.
    newest_first = Window.partitionBy("merchant_id").orderBy(F.desc("lsn"))
    type1 = (events.withColumn("_rank", F.row_number().over(newest_first))
             .where("_rank = 1").select("merchant_id", *MERCHANT_TYPE1))
    attrs = F.struct(*MERCHANT_TYPE2)
    changes = (events
               .withColumn("_attrs", attrs)
               .withColumn("_prev", F.lag("_attrs").over(by_merchant))
               # Keep only events that changed a Type 2 attribute: each starts a version.
               .where(F.col("_prev").isNull() | ~F.col("_attrs").eqNullSafe(F.col("_prev"))))
    next_from = F.lead("valid_from").over(by_merchant)
    versions = (changes
                .withColumn("version_number", F.row_number().over(by_merchant))
                # Version 1 starts when the merchant was created: history before CDC
                # began is unknown, so the snapshot state is assumed to have held since
                # then. Later versions start at the commit time of the change.
                .withColumn("valid_from", F.when(F.col("version_number") == 1, F.col("created_at"))
                            .otherwise(F.col("committed_at")))
                # Half-open [valid_from, valid_to): a timestamp falls in exactly one version.
                .withColumn("valid_to", F.coalesce(next_from, F.lit(OPEN_END).cast("timestamp")))
                .withColumn("is_current", next_from.isNull())
                .drop(*MERCHANT_TYPE1)
                .join(type1, "merchant_id", "left")
                .withColumn("is_inferred", F.lit(False)))

    # Late-arriving dimension: payments can reach silver before their merchant
    # does. Give such merchants a placeholder ("inferred member") so the fact row
    # still has a key; the next run replaces it and re-keys those payments.
    referenced = src["payments"].select("merchant_id").distinct()
    inferred = (referenced.join(versions.select("merchant_id"), "merchant_id", "left_anti")
                .select("merchant_id",
                        F.lit(UNKNOWN).alias("merchant_name"), F.lit(None).cast("string").alias("gstin"),
                        *[F.lit(None).cast("int" if c == "mdr_bps" else "string").alias(c)
                          for c in MERCHANT_TYPE2],
                        F.lit(1).alias("version_number"),
                        F.lit(FAR_PAST).cast("timestamp").alias("valid_from"),
                        F.lit(OPEN_END).cast("timestamp").alias("valid_to"),
                        F.lit(True).alias("is_current"), F.lit(True).alias("is_inferred")))
    columns = ["merchant_id", *MERCHANT_TYPE1, *MERCHANT_TYPE2,
               "version_number", "valid_from", "valid_to", "is_current", "is_inferred"]
    return (versions.select(*columns).unionByName(inferred.select(*columns))
            # Deterministic surrogate key: the same version gets the same key on every rebuild.
            .select(F.xxhash64("merchant_id", "valid_from").alias("merchant_sk"), *columns))


def build_type1_dim(silver, referenced, key, sk, columns, string_columns):
    """SCD Type 1 dimension plus inferred members for keys facts use but silver lacks yet."""
    real = silver.select(key, *columns).withColumn("is_inferred", F.lit(False))
    missing = referenced.join(real.select(key), key, "left_anti")
    placeholder = missing.select(
        key, *[(F.lit(UNKNOWN) if c in string_columns else F.lit(None)).cast(real.schema[c].dataType).alias(c)
               for c in columns],
        F.lit(True).alias("is_inferred"))
    # The key depends only on the natural key, so an inferred row and the real
    # row that later replaces it have the same key: facts need no re-keying.
    return real.unionByName(placeholder).select(F.xxhash64(key).alias(sk), "*")


def build_dim_customer(src):
    return build_type1_dim(
        src["customers"], src["payments"].select("customer_id").distinct(), "customer_id", "customer_sk",
        ["email_hash", "phone_last4", "city", "state", "kyc_status", "created_at"],
        string_columns={"city", "state", "kyc_status"})


def build_dim_payment_method(src):
    methods = src["payment_methods"].withColumnRenamed("_is_deleted", "is_deleted")
    return build_type1_dim(
        methods, src["payments"].select("payment_method_id").distinct(), "payment_method_id",
        "payment_method_sk",
        ["customer_id", "method_type", "provider", "card_network", "card_last4", "is_active",
         "is_deleted", "created_at"],
        string_columns={"method_type", "provider"})


# ----------------------------------------------------------------------------- facts

def merchant_version_at(facts, ts_col, dim_merchant):
    """Point-in-time join: the merchant version whose [valid_from, valid_to) contains ts."""
    m = dim_merchant.select("merchant_id", "merchant_sk", "mdr_bps", "valid_from", "valid_to").alias("m")
    return (facts.alias("f")
            .join(m, (F.col("f.merchant_id") == F.col("m.merchant_id"))
                  & (F.col(f"f.{ts_col}") >= F.col("m.valid_from"))
                  & (F.col(f"f.{ts_col}") < F.col("m.valid_to")), "left")
            .select("f.*", "m.merchant_sk", "m.mdr_bps"))


def build_fact_payments(payments, refunds, dim_merchant):
    by_order = Window.partitionBy("merchant_id", "merchant_order_id").orderBy("created_at", "payment_id")
    refund_totals = (refunds.where("NOT _is_deleted").groupBy("payment_id").agg(
        F.sum(F.when(F.col("status") == "processed", F.col("amount_paise")).otherwise(0)).alias("refunded_paise"),
        F.count(F.when(F.col("status") == "processed", True)).alias("refund_count")))
    p = (payments.where("NOT _is_deleted")
         .withColumn("attempt_number", F.row_number().over(by_order)))
    p = merchant_version_at(p, "created_at", dim_merchant)
    captured = F.col("status") == "captured"
    # Money stays in integer paise: fee = amount × rate / 10,000, rounded half up.
    fee = F.expr("(amount_paise * mdr_bps + 5000) div 10000")

    def seconds(later, earlier):
        return (F.unix_micros(later) - F.unix_micros(earlier)) / 1e6

    return (p.join(refund_totals, "payment_id", "left")
            .select(
                "payment_id", "merchant_order_id", "attempt_number",
                ist_date_key("created_at").alias("date_key"),
                "merchant_sk",
                F.xxhash64("customer_id").alias("customer_sk"),
                F.xxhash64("payment_method_id").alias("payment_method_sk"),
                "status", "failure_reason", "amount_paise",
                F.col("mdr_bps").alias("mdr_bps_applied"),
                F.when(captured, fee).otherwise(0).alias("fee_paise"),
                F.when(captured, F.col("amount_paise") - fee).otherwise(0).alias("net_settlement_paise"),
                F.coalesce("refunded_paise", F.lit(0)).alias("refunded_paise"),
                F.coalesce("refund_count", F.lit(0)).alias("refund_count"),
                captured.cast("int").alias("is_captured"),
                (F.col("status") == "failed").cast("int").alias("is_failed"),
                seconds("authorized_at", "created_at").alias("seconds_to_authorize"),
                seconds("captured_at", "authorized_at").alias("seconds_to_capture"),
                "created_at", "authorized_at", "captured_at",
                F.current_timestamp().alias("_gold_updated_at")))


def build_fact_refunds(src, dim_merchant):
    r = (src["refunds"].where("NOT _is_deleted")
         .join(src["payments"].select("payment_id", "merchant_id", "customer_id", "payment_method_id"),
               "payment_id", "left"))
    r = merchant_version_at(r, "created_at", dim_merchant)
    return r.select(
        "refund_id", "payment_id", ist_date_key("created_at").alias("date_key"), "merchant_sk",
        F.xxhash64("customer_id").alias("customer_sk"),
        F.xxhash64("payment_method_id").alias("payment_method_sk"),
        "amount_paise", "status", "reason", "created_at",
        F.current_timestamp().alias("_gold_updated_at"))


# ----------------------------------------------------------------------------- writes

def replace_table(df, name, properties=COMMON_PROPERTIES, partition_col=None):
    writer = df.writeTo(name).using("iceberg")
    for key, value in properties.items():
        writer = writer.tableProperty(key, value)
    if partition_col:
        writer = writer.partitionedBy(partitioning.days(partition_col))
    writer.createOrReplace()  # atomic: readers see the old table or the new one, never half


def payments_to_recompute(spark, src, state, dim_merchant, old_merchant_sks):
    """Payment ids whose fact row may differ from what gold has now."""
    payments, refunds = src["payments"], src["refunds"]
    changed = (payments.where(F.unix_micros("_silver_updated_at") > state["payments_watermark_us"])
               .select("payment_id")
               .union(refunds.where(F.unix_micros("_silver_updated_at") > state["refunds_watermark_us"])
                      .select("payment_id")))
    # New merchant versions (a change that arrived late, or an inferred merchant
    # that is now known): payments from that version's start may have the wrong
    # version and fee.
    old = spark.createDataFrame([(sk,) for sk in old_merchant_sks], "merchant_sk bigint")
    new_versions = dim_merchant.join(old, "merchant_sk", "left_anti").select("merchant_id", "valid_from")
    affected = (payments.join(new_versions, "merchant_id")
                .where(F.col("created_at") >= F.col("valid_from")).select("payment_id"))
    return changed.union(affected).distinct(), new_versions.count()


def main():
    parser = argparse.ArgumentParser()
    lakehouse_env.add_flag(parser, "--full-refresh")
    lakehouse_env.add_flag(parser, "--check", help="compare fact_payments with a full recomputation; write nothing")
    args = lakehouse_env.parse_args(parser)
    if args.full_refresh and args.check:
        parser.error("--full-refresh and --check are mutually exclusive")

    spark = lakehouse_env.spark_session("gold-star")
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD}")
    fact_name = f"{GOLD}.fact_payments"
    state_name = f"{GOLD}.pipeline_state"

    if args.check:
        state = spark.table(state_name).first()
        src, _ = inputs(spark, state["input_snapshots"])
        lakehouse_env.finish(failed=check(spark, src, fact_name) != 0)
        return

    src, snapshot_ids = inputs(spark)

    # Merchant versions as gold had them before this run (to detect new ones).
    old_dim = f"{GOLD}.dim_merchant"
    old_merchant_sks = ([r["merchant_sk"] for r in spark.table(old_dim).select("merchant_sk").collect()]
                        if spark.catalog.tableExists(old_dim) else [])

    replace_table(build_dim_date(spark), f"{GOLD}.dim_date")
    replace_table(build_dim_merchant(src), old_dim)
    replace_table(build_dim_customer(src), f"{GOLD}.dim_customer")
    replace_table(build_dim_payment_method(src), f"{GOLD}.dim_payment_method")
    # Pinned to the version just written. (Also: a spark.table() DataFrame carries
    # a lazy TableReference that Iceberg's MERGE can't plan inside a MERGE source.)
    dim_merchant = pinned(spark, old_dim)

    state = (spark.table(state_name).first() if spark.catalog.tableExists(state_name) else None)
    # (A Row is a tuple: `"x" in row` tests the values, so check the field names.)
    full = (args.full_refresh or state is None or "input_snapshots" not in state.asDict()
            or not spark.catalog.tableExists(fact_name))

    if full:
        replace_table(build_fact_payments(src["payments"], src["refunds"], dim_merchant),
                      fact_name, FACT_PROPERTIES, partition_col="created_at")
        summary = "full refresh"
    else:
        ids, n_new_versions = payments_to_recompute(spark, src, state, dim_merchant, old_merchant_sks)
        # Attempt numbers depend on the other attempts of the same order, so
        # recompute whole orders, not just the changed attempts.
        orders = (src["payments"].join(ids, "payment_id", "left_semi")
                  .select("merchant_id", "merchant_order_id").distinct())
        subset = src["payments"].join(orders, ["merchant_id", "merchant_order_id"], "left_semi")
        updates = build_fact_payments(subset, src["refunds"], dim_merchant).cache()
        n = updates.count()
        updates.createOrReplaceTempView("fact_payments_updates")
        cols = updates.columns
        spark.sql(f"""
            MERGE INTO {fact_name} t USING fact_payments_updates s ON t.payment_id = s.payment_id
            WHEN MATCHED THEN UPDATE SET {", ".join(f"t.{c} = s.{c}" for c in cols)}
            WHEN NOT MATCHED THEN INSERT ({", ".join(cols)}) VALUES ({", ".join(f"s.{c}" for c in cols)})
        """)
        updates.unpersist()
        summary = f"incremental: {n} payments recomputed ({n_new_versions} new merchant versions)"

    replace_table(build_fact_refunds(src, dim_merchant), f"{GOLD}.fact_refunds")

    # Saved last: if the run dies before this, the next run starts from the old
    # watermarks and recomputes the same rows again, which the MERGE makes harmless.
    # The watermark is the newest change *in the data read*, never the clock: a
    # silver batch still committing during this run has an older timestamp than
    # "now" but wasn't visible, and must be picked up next time.
    # Stored as integer microseconds to avoid any timezone conversion on the way.
    marks = {t: src[t].agg(F.max(F.unix_micros("_silver_updated_at"))).first()[0]
             for t in ["payments", "refunds"]}
    spark.createDataFrame(
        [(marks["payments"], marks["refunds"], snapshot_ids)],
        "payments_watermark_us bigint, refunds_watermark_us bigint, input_snapshots map<string, bigint>") \
        .withColumn("run_at", F.current_timestamp()).writeTo(state_name).createOrReplace()

    report(spark, summary)


def check(spark, src, fact_name):
    """Full recomputation vs the table, ignoring the _gold_updated_at timestamp."""
    expected = build_fact_payments(src["payments"], src["refunds"], pinned(spark, f"{GOLD}.dim_merchant"))
    cols = [c for c in expected.columns if c != "_gold_updated_at"]
    expected, actual = expected.select(*cols), spark.table(fact_name).select(*cols)
    only_expected, only_actual = expected.exceptAll(actual).count(), actual.exceptAll(expected).count()
    print(f"fact_payments vs full recomputation: {only_expected} rows only in recomputation, "
          f"{only_actual} rows only in table")
    ok = only_expected == 0 and only_actual == 0
    print("CHECK:", "PASS, incremental result equals a full rebuild" if ok else "FAIL")
    return 0 if ok else 1


def report(spark, summary):
    g = GOLD
    rows = spark.sql(f"""
        SELECT 'dim_merchant' AS t, count(*) AS rows,
               concat(count_if(is_current), ' merchants, ', count_if(NOT is_current), ' past versions, ',
                      count_if(is_inferred), ' inferred') AS detail FROM {g}.dim_merchant
        UNION ALL SELECT 'dim_customer', count(*), concat(count_if(is_inferred), ' inferred') FROM {g}.dim_customer
        UNION ALL SELECT 'dim_payment_method', count(*), concat(count_if(is_inferred), ' inferred') FROM {g}.dim_payment_method
        UNION ALL SELECT 'dim_date', count(*), concat(min(date), ' to ', max(date)) FROM {g}.dim_date
        UNION ALL SELECT 'fact_payments', count(*), concat(count_if(merchant_sk IS NULL), ' without merchant version, ',
               count_if(d.date_key IS NULL), ' outside calendar')
               FROM {g}.fact_payments f LEFT JOIN {g}.dim_date d USING (date_key)
        UNION ALL SELECT 'fact_refunds', count(*), concat(count_if(merchant_sk IS NULL), ' without merchant version')
               FROM {g}.fact_refunds
    """).collect()
    print(f"gold run: {summary}")
    for r in rows:
        print(f"  {r['t']:20} {r['rows']:>9}  {r['detail']}")


if __name__ == "__main__":
    main()
