"""Data-quality checks for silver and gold, safe to run while data is streaming.

Every check counts violations. Severity decides what a violation means:
    error  the job exits non-zero, so the Airflow task fails and alerts
    warn   recorded, nothing fails (worth a look, may resolve itself)

Each run appends its results to lakehouse.ops.dq_results, so check history
can be charted and alerted on.

Checks against the source compare only *settled* rows: rows whose last change
in Postgres is older than SETTLE_MINUTES. Anything newer may still be in
flight through Debezium, bronze and silver, so it isn't a violation yet. That
is what makes these checks usable on a live system, unlike the strict
reconcile_silver.py, which needs the generator stopped.

    spark-submit dq_checks.py
"""
import os
import sys
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import partitioning

from silver_cdc import TABLES

SILVER, GOLD, OPS = "lakehouse.silver", "lakehouse.gold", "lakehouse.ops"
SETTLE_MINUTES = 10
MAX_SILVER_LAG_S = 300        # silver may trail Postgres by at most 5 minutes
MAX_GOLD_AGE_S = 30 * 60      # gold must have run in the last 30 minutes
JDBC_URL = f"jdbc:postgresql://{os.getenv('PGHOST', 'postgres')}:5432/{os.getenv('POSTGRES_DB', 'payments')}"
JDBC_PROPS = {"user": "debezium", "password": os.environ["DEBEZIUM_PASSWORD"], "driver": "org.postgresql.Driver"}

# (name, severity, SQL returning one row: violations [, details])
SQL_CHECKS = [
    *[(f"silver_{t}_unique_key", "error",
       f"SELECT count(*) - count(DISTINCT {pk}) AS violations FROM {SILVER}.{t}")
      for t, (pk, _) in TABLES.items()],
    ("gold_fact_payments_unique_payment", "error",
     f"SELECT count(*) - count(DISTINCT payment_id) AS violations FROM {GOLD}.fact_payments"),
    ("gold_fact_payments_has_merchant_version", "error",
     f"""SELECT count(*) AS violations FROM {GOLD}.fact_payments f
         LEFT ANTI JOIN {GOLD}.dim_merchant d USING (merchant_sk)"""),
    ("gold_fact_payments_has_customer", "error",
     f"""SELECT count(*) AS violations FROM {GOLD}.fact_payments f
         LEFT ANTI JOIN {GOLD}.dim_customer d USING (customer_sk)"""),
    ("gold_fact_payments_has_payment_method", "error",
     f"""SELECT count(*) AS violations FROM {GOLD}.fact_payments f
         LEFT ANTI JOIN {GOLD}.dim_payment_method d USING (payment_method_sk)"""),
    ("gold_fact_payments_date_in_calendar", "error",
     f"""SELECT count(*) AS violations FROM {GOLD}.fact_payments f
         LEFT ANTI JOIN {GOLD}.dim_date d USING (date_key)"""),
    ("gold_fact_payments_amounts_consistent", "error",
     f"""SELECT count_if(amount_paise <= 0 OR fee_paise > amount_paise OR refunded_paise > amount_paise
                         OR (is_captured = 1 AND captured_at IS NULL)) AS violations
         FROM {GOLD}.fact_payments"""),
    ("gold_dim_merchant_one_current_version", "error",
     f"""SELECT count(*) AS violations FROM (
           SELECT merchant_id FROM {GOLD}.dim_merchant GROUP BY merchant_id HAVING count_if(is_current) <> 1)"""),
    ("gold_dim_merchant_no_gaps_or_overlaps", "error",
     f"""SELECT count_if(next_from IS NOT NULL AND valid_to <> next_from) + count_if(valid_from >= valid_to)
                AS violations
         FROM (SELECT *, lead(valid_from) OVER (PARTITION BY merchant_id ORDER BY valid_from) AS next_from
               FROM {GOLD}.dim_merchant)"""),
    *[(f"gold_{d}_unique_key", "error",
       f"SELECT count(*) - count(DISTINCT {sk}) AS violations FROM {GOLD}.{d}")
      for d, sk in [("dim_merchant", "merchant_sk"), ("dim_customer", "customer_sk"),
                    ("dim_payment_method", "payment_method_sk")]],
    # A late dimension normally arrives within a minute or two. Still inferred
    # after 30 minutes means it probably never will: worth investigating.
    ("gold_inferred_merchants_resolved", "warn",
     f"""SELECT count(*) AS violations FROM {GOLD}.fact_payments f JOIN {GOLD}.dim_merchant d USING (merchant_sk)
         WHERE d.is_inferred AND f.created_at < current_timestamp() - INTERVAL 30 MINUTES"""),
    ("gold_freshness", "error",
     f"""SELECT CAST(max(run_at) < current_timestamp() - INTERVAL {MAX_GOLD_AGE_S} SECONDS AS INT) AS violations,
                concat('last gold run ', CAST(max(run_at) AS STRING)) AS details
         FROM {GOLD}.pipeline_state"""),
    # A business sanity check: today's success rate far outside the normal range
    # usually means an upstream outage or a pipeline bug, not a real trend.
    ("gold_success_rate_today_plausible", "warn",
     f"""SELECT CAST(count(*) >= 200 AND NOT avg(is_captured) BETWEEN 0.70 AND 0.99 AS INT) AS violations,
                concat(count(*), ' attempts today, success ', round(100 * avg(is_captured), 1), '%') AS details
         FROM {GOLD}.fact_payments
         WHERE date_key = CAST(date_format(from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'), 'yyyyMMdd') AS INT)"""),
]


def source_checks(spark, table, pk):
    """Settled rows must match the source; silver must not trail it by much."""
    pg = (spark.read.jdbc(JDBC_URL, f"(SELECT {pk}, updated_at FROM {table}) t", properties=JDBC_PROPS)
          .withColumnRenamed("updated_at", "pg_updated_at"))
    silver = spark.table(f"{SILVER}.{table}").select(pk, "updated_at", "_is_deleted", "_silver_updated_at")
    cutoff = F.current_timestamp() - F.expr(f"INTERVAL {SETTLE_MINUTES} MINUTES")

    live = silver.where("NOT _is_deleted")
    # Source rows whose current version silver hasn't applied (yet).
    unapplied = (pg.join(live, pk, "left")
                 .where(F.col("updated_at").isNull() | (F.col("updated_at") < F.col("pg_updated_at"))))
    # In Postgres for a while, but missing from silver or at an older version there.
    stale = unapplied.where(F.col("pg_updated_at") < cutoff).count()
    # In silver for a while, gone from Postgres: a delete that never arrived
    # (or one that is still in flight, hence only a warning).
    extra = live.where(F.col("_silver_updated_at") < cutoff).join(pg, pk, "left_anti").count()
    # Freshness = how long the oldest unapplied change has been waiting.
    # Not max(source) - max(silver): after an idle spell that difference is the
    # idle time, so it raised a false alarm the moment traffic resumed.
    lag_s = (unapplied.agg((F.unix_micros(F.current_timestamp())
                            - F.unix_micros(F.min("pg_updated_at"))) / 1e6).first()[0] or 0.0)
    return [
        (f"silver_{table}_settled_rows_match_source", "error", stale,
         f"rows changed > {SETTLE_MINUTES} min ago that are missing or stale in silver"),
        (f"silver_{table}_no_rows_deleted_at_source", "warn", extra, "live in silver, absent in Postgres"),
        (f"silver_{table}_freshness", "error", int(lag_s > MAX_SILVER_LAG_S),
         f"silver trails Postgres by {max(lag_s, 0):.0f} s (limit {MAX_SILVER_LAG_S} s)"),
    ]


def main():
    spark = (SparkSession.builder.appName("dq-checks")
             .config("spark.sql.catalog.lakehouse.jdbc.password", os.environ["ICEBERG_CATALOG_PASSWORD"])
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    results = []
    for table, (pk, _) in TABLES.items():
        results += source_checks(spark, table, pk)
    # Privacy: no raw personal data columns may ever appear in silver customers.
    raw_pii = {"full_name", "email", "phone"} & set(spark.table(f"{SILVER}.customers").columns)
    results.append(("silver_customers_no_raw_pii", "error", len(raw_pii),
                    ", ".join(sorted(raw_pii)) or "only email_hash and phone_last4"))
    for name, severity, sql in SQL_CHECKS:
        row = spark.sql(sql).first()
        results.append((name, severity, int(row["violations"] or 0),
                        row["details"] if "details" in row.asDict() else None))

    run_id = uuid.uuid4().hex[:12]
    df = (spark.createDataFrame(results, "check_name string, severity string, violations bigint, details string")
          .select(F.lit(run_id).alias("run_id"), F.current_timestamp().alias("ran_at"), "*",
                  (F.col("violations") == 0).alias("passed")))
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {OPS}")
    if spark.catalog.tableExists(f"{OPS}.dq_results"):
        df.writeTo(f"{OPS}.dq_results").append()
    else:
        df.writeTo(f"{OPS}.dq_results").partitionedBy(partitioning.days("ran_at")).create()

    failed_errors = [r for r in results if r[1] == "error" and r[2]]
    failed_warns = [r for r in results if r[1] == "warn" and r[2]]
    for name, severity, violations, details in results:
        status = "PASS" if not violations else ("FAIL" if severity == "error" else "WARN")
        print(f"{status:4}  {severity:5}  {name:48} {violations:>7}  {details or ''}")
    print(f"\nrun {run_id}: {len(results)} checks, {len(failed_errors)} errors, {len(failed_warns)} warnings")
    spark.stop()
    sys.exit(1 if failed_errors else 0)


if __name__ == "__main__":
    main()
