"""Gold: SCD Type 2 merchant history, point-in-time fees, India's calendar (gold_star.py)."""
from datetime import date, datetime, timedelta

from pyspark.sql import functions as F

from gold_star import (FAR_PAST, OPEN_END, UNKNOWN, build_dim_date, build_dim_merchant,
                       build_fact_payments, ist_date_key)

MERCHANT = ("struct<merchant_id:bigint,merchant_name:string,category:string,city:string,state:string,"
            "pricing_plan:string,mdr_bps:int,status:string,created_at:string,updated_at:string,gstin:string>")
BRONZE_MERCHANTS = f"after {MERCHANT}, source struct<lsn:bigint,ts_ms:bigint>, op string"
CREATED = "2026-01-01T00:00:00Z"
T_PLAN_CHANGE = datetime(2026, 11, 21, 15, 28)


def ms(ts):
    return int((ts - datetime(1970, 1, 1)).total_seconds() * 1000)


def merchant_event(op, lsn, ts, plan="standard", mdr=200, name="Chai Point", updated="x"):
    after = (1, name, "food", "Pune", "MH", plan, mdr, "active", CREATED, updated, None)
    return after, (lsn, ms(ts)), op


def dim_merchant(spark, extra_payment_merchants=()):
    history = spark.createDataFrame([
        merchant_event("r", 100, datetime(2026, 11, 21, 9, 0)),                       # snapshot
        merchant_event("u", 200, datetime(2026, 11, 21, 12, 0), updated="y"),         # no Type 2 change
        merchant_event("u", 300, T_PLAN_CHANGE, plan="growth", mdr=180),              # new version
        merchant_event("u", 300, T_PLAN_CHANGE, plan="growth", mdr=180),              # re-delivery
        merchant_event("u", 400, datetime(2026, 11, 21, 16, 0), plan="growth", mdr=180,
                       name="Chai Point Cafe"),                                       # Type 1 only
    ], BRONZE_MERCHANTS)
    payments = spark.createDataFrame([(1,), *[(m,) for m in extra_payment_merchants]], "merchant_id bigint")
    return build_dim_merchant({"bronze_merchants": history, "payments": payments})


def test_scd2_versions_only_for_type2_changes(spark):
    rows = sorted(dim_merchant(spark).collect(), key=lambda r: r["version_number"])
    assert [(r["pricing_plan"], r["mdr_bps"]) for r in rows] == [("standard", 200), ("growth", 180)]
    v1, v2 = rows
    assert v1["valid_from"] == datetime(2026, 1, 1)          # version 1 starts at creation
    assert v1["valid_to"] == v2["valid_from"] == T_PLAN_CHANGE  # no gap, no overlap
    assert v2["valid_to"] == datetime.fromisoformat(OPEN_END)
    assert (v1["is_current"], v2["is_current"]) == (False, True)


def test_type1_name_is_current_on_every_version(spark):
    assert {r["merchant_name"] for r in dim_merchant(spark).collect()} == {"Chai Point Cafe"}


def test_surrogate_keys_are_stable_across_rebuilds(spark):
    first = {r["version_number"]: r["merchant_sk"] for r in dim_merchant(spark).collect()}
    second = {r["version_number"]: r["merchant_sk"] for r in dim_merchant(spark).collect()}
    assert first == second and len(set(first.values())) == 2


def test_unknown_merchant_gets_an_inferred_member(spark):
    inferred = [r for r in dim_merchant(spark, extra_payment_merchants=[2]).collect() if r["merchant_id"] == 2]
    assert len(inferred) == 1
    row = inferred[0]
    assert row["is_inferred"] and row["merchant_name"] == UNKNOWN and row["mdr_bps"] is None
    assert row["valid_from"] == datetime.fromisoformat(FAR_PAST)


PAYMENTS = ("payment_id bigint, merchant_id bigint, customer_id bigint, payment_method_id bigint, "
            "merchant_order_id string, status string, failure_reason string, amount_paise bigint, "
            "created_at timestamp, authorized_at timestamp, captured_at timestamp, _is_deleted boolean")
REFUNDS = "payment_id bigint, status string, amount_paise bigint, _is_deleted boolean"


def payment(pid, created_at, amount, order="o1", status="captured"):
    return (pid, 1, 7, 9, order, status, None if status != "failed" else "declined", amount,
            created_at, created_at, created_at if status == "captured" else None, False)


def facts(spark, payments, refunds=()):
    rows = build_fact_payments(spark.createDataFrame(payments, PAYMENTS),
                               spark.createDataFrame(list(refunds), REFUNDS),
                               dim_merchant(spark)).collect()
    return {r["payment_id"]: r for r in rows}


def test_fee_uses_the_rate_in_force_at_payment_time_half_open(spark):
    just_before = T_PLAN_CHANGE - timedelta(microseconds=1)
    rows = facts(spark, [payment(1, just_before, 75_000, order="a"),
                         payment(2, T_PLAN_CHANGE, 75_000, order="b")])  # exactly at the change
    assert (rows[1]["mdr_bps_applied"], rows[1]["fee_paise"]) == (200, 1_500)  # ₹750 × 2.00%
    assert (rows[2]["mdr_bps_applied"], rows[2]["fee_paise"]) == (180, 1_350)  # ₹750 × 1.80%


def test_fee_is_integer_paise_rounded_half_up(spark):
    rows = facts(spark, [payment(1, datetime(2026, 11, 21, 10), 50, order="a"),       # 50 × 2% = 1.0
                         payment(2, datetime(2026, 11, 21, 10), 25, order="b"),       # 0.5 → 1
                         payment(3, datetime(2026, 11, 21, 10), 24, order="c")])      # 0.48 → 0
    assert [rows[i]["fee_paise"] for i in (1, 2, 3)] == [1, 1, 0]
    assert rows[2]["net_settlement_paise"] == 24


def test_attempts_numbered_within_an_order_and_refunds_summed(spark):
    t = datetime(2026, 11, 21, 10)
    rows = facts(spark,
                 [payment(1, t, 10_000, order="o1", status="failed"),
                  payment(2, t + timedelta(seconds=30), 10_000, order="o1")],
                 refunds=[(2, "processed", 4_000, False), (2, "failed", 6_000, False)])
    assert (rows[1]["attempt_number"], rows[2]["attempt_number"]) == (1, 2)
    assert rows[1]["fee_paise"] == 0                                    # no fee on a failed attempt
    assert (rows[2]["refunded_paise"], rows[2]["refund_count"]) == (4_000, 1)  # processed only


def test_business_day_is_the_india_date(spark):
    # 19:00 UTC on 21 Nov is 00:30 IST on 22 Nov.
    df = spark.createDataFrame([(datetime(2026, 11, 21, 19, 0),)], "ts timestamp")
    assert df.select(ist_date_key(F.col("ts"))).first()[0] == 20261122


def test_dim_date_uses_indias_april_to_march_financial_year(spark):
    days = {r["date"]: r for r in build_dim_date(spark).collect()}
    assert len(days) == 1461  # 2024-01-01 to 2027-12-31
    assert (days[date(2026, 3, 31)]["fiscal_year"], days[date(2026, 3, 31)]["fiscal_quarter"]) == ("FY2025-26", 4)
    assert (days[date(2026, 4, 1)]["fiscal_year"], days[date(2026, 4, 1)]["fiscal_quarter"]) == ("FY2026-27", 1)
    assert days[date(2026, 11, 21)]["fiscal_quarter"] == 3
    assert days[date(2026, 11, 21)]["is_weekend"] and not days[date(2026, 11, 20)]["is_weekend"]
