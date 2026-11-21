"""Compare every silver table with Postgres, row by row and column by column.

Reads each source table over JDBC, applies the same column rules as the silver
job (types, PII hashing), and full-outer-joins it with the non-deleted silver
rows on the primary key:

    missing     in Postgres, not in silver
    extra       in silver, not in Postgres (a delete that never arrived)
    mismatched  in both, but at least one column differs

This checks the whole pipeline end to end: Postgres -> Debezium -> Kafka ->
bronze -> silver. Run it with the generator stopped and silver caught up.

    spark-submit --py-files silver_cdc.py reconcile_silver.py
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from silver_cdc import SILVER, TABLES, pii_hasher, silver_columns

JDBC_URL = f"jdbc:postgresql://{os.getenv('PGHOST', 'postgres')}:5432/{os.getenv('POSTGRES_DB', 'payments')}"
JDBC_PROPS = {
    # The read-only CDC role: SELECT on the payments tables and nothing else.
    "user": "debezium",
    "password": os.environ["DEBEZIUM_PASSWORD"],
    "driver": "org.postgresql.Driver",
}


def read_postgres(spark, table, pk):
    """Read a table in 4 parallel JDBC queries, split on primary-key ranges."""
    bounds = spark.read.jdbc(JDBC_URL, f"(SELECT min({pk}) AS lo, max({pk}) AS hi FROM {table}) b",
                             properties=JDBC_PROPS).first()
    return spark.read.jdbc(JDBC_URL, table, column=pk, lowerBound=bounds["lo"], upperBound=bounds["hi"] + 1,
                           numPartitions=4, properties=JDBC_PROPS)


def reconcile(spark, table, email_hash):
    pk, _ = TABLES[table]
    pg = read_postgres(spark, table, pk)
    pg = pg.select(F.struct(*pg.columns).alias("row"))
    expected = pg.select(*silver_columns(pg, "row", table, email_hash))
    columns = [c for c in expected.columns if c != pk]
    actual = spark.table(f"{SILVER}.{table}").where("NOT _is_deleted").select(pk, *columns)

    joined = (expected.withColumn("_in_pg", F.lit(True)).alias("e")
              .join(actual.withColumn("_in_silver", F.lit(True)).alias("a"), pk, "full_outer"))
    both = F.col("_in_pg") & F.col("_in_silver")
    differs = {c: both & ~F.col(f"e.{c}").eqNullSafe(F.col(f"a.{c}")) for c in columns}
    any_differs = F.lit(False)
    for d in differs.values():
        any_differs = any_differs | d

    def count_if(cond):
        return F.sum(F.when(cond, 1).otherwise(0))

    stats = joined.agg(
        count_if(F.col("_in_pg")).alias("postgres"),
        count_if(F.col("_in_silver")).alias("silver"),
        count_if(F.col("_in_silver").isNull()).alias("missing"),
        count_if(F.col("_in_pg").isNull()).alias("extra"),
        count_if(any_differs).alias("mismatched"),
        *[count_if(d).alias(f"col:{c}") for c, d in differs.items()],
    ).first().asDict()
    bad_columns = [k[4:] for k, v in stats.items() if k.startswith("col:") and v]
    deleted = spark.table(f"{SILVER}.{table}").where("_is_deleted").count()
    return stats, bad_columns, deleted, len(columns) + 1


def main():
    spark = (SparkSession.builder.appName("reconcile-silver")
             .config("spark.sql.catalog.lakehouse.jdbc.password", os.environ["ICEBERG_CATALOG_PASSWORD"])
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    email_hash = pii_hasher()

    header = (f"{'table':16} {'postgres':>9} {'silver':>9} {'missing':>8} {'extra':>6} "
              f"{'mismatch':>9} {'columns':>8} {'deleted':>8}")
    print(header + "\n" + "-" * len(header))
    all_ok = True
    for table in TABLES:
        s, bad_columns, deleted, n_columns = reconcile(spark, table, email_hash)
        ok = not (s["missing"] or s["extra"] or s["mismatched"])
        all_ok &= ok
        detail = "OK" if ok else f"MISMATCH in {', '.join(bad_columns) or 'keys'}"
        print(f"{table:16} {s['postgres']:>9} {s['silver']:>9} {s['missing']:>8} {s['extra']:>6} "
              f"{s['mismatched']:>9} {n_columns:>8} {deleted:>8}  {detail}")
    print("\ncolumns = columns compared per row; deleted = soft-deleted rows kept in silver")
    print("RESULT:", "PASS, silver matches Postgres" if all_ok else "FAIL")
    spark.stop()
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
