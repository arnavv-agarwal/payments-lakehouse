"""Silver: reducing CDC events to the current state of each key (silver_cdc.latest_per_key)."""
from datetime import datetime

from silver_cdc import latest_per_key

PAYMENT = "struct<payment_id:bigint,status:string,amount_paise:bigint,created_at:string,updated_at:string>"
BRONZE_SCHEMA = (f"before {PAYMENT}, after {PAYMENT}, source struct<lsn:bigint,ts_ms:bigint>, "
                 "op string, kafka_offset bigint")
COMMIT_MS = 1_795_000_000_000  # 2026-11-18 11:06:40 UTC, plus the LSN, so later changes commit later


def event(op, lsn, offset, payment_id, status="created"):
    """One Debezium change event as bronze stores it."""
    image = (payment_id, status, 75_000, "2026-11-21T10:00:00.123456Z", "2026-11-21T10:05:00Z")
    after = None if op == "d" else image
    before = (payment_id, None, None, None, None) if op == "d" else None  # Debezium: key only
    return before, after, (lsn, COMMIT_MS + lsn), op, offset


def silver(spark, events):
    df = spark.createDataFrame(events, BRONZE_SCHEMA)
    return {r["payment_id"]: r for r in latest_per_key(df, "payments", email_hash=None).collect()}


def test_newest_change_wins_by_wal_position_not_arrival_order(spark):
    rows = silver(spark, [
        event("c", lsn=10, offset=0, payment_id=1, status="created"),
        event("u", lsn=30, offset=1, payment_id=1, status="captured"),
        event("u", lsn=20, offset=2, payment_id=1, status="authorized"),  # older, arrived last
    ])
    assert rows[1]["status"] == "captured"
    assert rows[1]["_lsn"] == 30


def test_duplicate_delivery_gives_one_row(spark):
    rows = silver(spark, [
        event("c", lsn=10, offset=0, payment_id=2),
        event("u", lsn=20, offset=1, payment_id=2, status="authorized"),
        event("u", lsn=20, offset=5, payment_id=2, status="authorized"),  # at-least-once re-delivery
    ])
    assert len(rows) == 1
    assert (rows[2]["status"], rows[2]["_lsn"]) == ("authorized", 20)


def test_delete_is_soft_and_keeps_the_last_known_row(spark):
    rows = silver(spark, [
        event("c", lsn=10, offset=0, payment_id=3, status="failed"),
        event("d", lsn=40, offset=1, payment_id=3),
    ])
    row = rows[3]
    assert row["_is_deleted"] and row["_op"] == "d" and row["_has_image"]
    assert row["status"] == "failed"  # the image before the delete, not NULLs
    assert row["_deleted_at"] == datetime(2026, 11, 18, 11, 6, 40, 40_000)  # the delete's commit time


def test_delete_with_no_earlier_image_keeps_the_key_from_before(spark):
    rows = silver(spark, [event("d", lsn=50, offset=0, payment_id=4)])
    assert rows[4]["payment_id"] == 4
    assert rows[4]["_is_deleted"] and not rows[4]["_has_image"]  # MERGE won't insert it


def test_debezium_timestamp_strings_become_timestamps(spark):
    rows = silver(spark, [event("c", lsn=10, offset=0, payment_id=5)])
    assert rows[5]["created_at"] == datetime(2026, 11, 21, 10, 0, 0, 123_456)
