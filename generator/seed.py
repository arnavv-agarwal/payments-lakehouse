"""`seed`: reference data plus N days of payment history, bulk-loaded with COPY.

Runs once against an empty database, inside a single transaction: either all of
it lands or none of it does.
"""
import logging
import random
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from faker import Faker
from psycopg import sql

from . import reference as ref

log = logging.getLogger(__name__)

MERCHANT_COLS = ["merchant_name", "category", "city", "state", "pricing_plan", "mdr_bps",
                 "created_at", "updated_at"]
CUSTOMER_COLS = ["full_name", "email", "phone", "city", "state", "kyc_status",
                 "created_at", "updated_at"]
METHOD_COLS = ["customer_id", "method_type", "provider", "card_network", "card_last4",
               "created_at", "updated_at"]
PAYMENT_COLS = ["merchant_id", "customer_id", "payment_method_id", "merchant_order_id",
                "idempotency_key", "amount_paise", "status", "failure_reason",
                "created_at", "authorized_at", "captured_at", "updated_at"]
REFUND_COLS = ["payment_id", "amount_paise", "status", "reason", "created_at", "updated_at"]


def seed(conn, *, merchants, customers, history_days, orders_per_day, random_seed):
    if conn.execute("SELECT EXISTS (SELECT 1 FROM merchants)").fetchone()[0]:
        raise SystemExit(
            "The database already has data, so it was seeded before.\n"
            "To start over: docker compose down -v && docker compose up -d --wait"
        )

    rng = random.Random(random_seed)
    fake = Faker("en_IN")
    fake.seed_instance(random_seed)
    now = datetime.now(timezone.utc)
    # Everything in the reference tables already existed when the history window opens.
    window_start = now - timedelta(days=history_days)
    started = time.monotonic()

    with conn.transaction():
        merchant_category = _load_merchants(conn, rng, fake, merchants, window_start)
        customer_created = _load_customers(conn, rng, fake, customers, window_start)
        methods = _load_methods(conn, rng, customer_created, window_start)
        refund_intents = _load_payments(conn, rng, merchant_category, methods,
                                        window_start, now, orders_per_day)
        _load_refunds(conn, refund_intents)

    conn.execute("ANALYZE")  # fresh planner statistics after a bulk load
    log.info("seed finished in %.1fs", time.monotonic() - started)


def _copy(conn, table, cols, rows):
    """Bulk insert with COPY: one round trip for all rows instead of one per row."""
    stmt = sql.SQL("COPY {} ({}) FROM STDIN").format(
        sql.Identifier(table), sql.SQL(", ").join(map(sql.Identifier, cols)))
    with conn.cursor() as cur, cur.copy(stmt) as copy:
        for row in rows:
            copy.write_row(row)
    log.info("%-16s %9d rows", table, len(rows))


def _load_merchants(conn, rng, fake, n, window_start):
    rows = []
    for _ in range(n):
        m = ref.merchant_row(rng, fake)
        m["created_at"] = m["updated_at"] = window_start - timedelta(days=rng.uniform(1, 730))
        rows.append([m[c] for c in MERCHANT_COLS])
    _copy(conn, "merchants", MERCHANT_COLS, rows)
    return dict(conn.execute("SELECT merchant_id, category FROM merchants").fetchall())


def _load_customers(conn, rng, fake, n, window_start):
    rows = []
    for _ in range(n):
        c = ref.customer_row(rng, fake)
        c["created_at"] = c["updated_at"] = window_start - timedelta(days=rng.uniform(1, 540))
        rows.append([c[col] for col in CUSTOMER_COLS])
    _copy(conn, "customers", CUSTOMER_COLS, rows)
    return dict(conn.execute("SELECT customer_id, created_at FROM customers").fetchall())


def _load_methods(conn, rng, customer_created, window_start):
    rows = []
    for customer_id, created in customer_created.items():
        for _ in range(ref.pick(rng, ref.METHODS_PER_CUSTOMER)):
            m = ref.method_row(rng, customer_id)
            added = min(created + timedelta(days=rng.uniform(0, 30)), window_start)
            m["created_at"] = m["updated_at"] = added
            rows.append([m[c] for c in METHOD_COLS])
    _copy(conn, "payment_methods", METHOD_COLS, rows)

    methods = defaultdict(list)
    for method_id, customer_id, method_type in conn.execute(
            "SELECT payment_method_id, customer_id, method_type FROM payment_methods"):
        methods[customer_id].append((method_id, method_type))
    return methods


def _load_payments(conn, rng, merchant_category, methods, window_start, now, orders_per_day):
    """Simulate every order in the window, including failed attempts and retries."""
    merchant_ids = list(merchant_category)
    merchant_cum = ref.cumulative(ref.popularity(m, ref.MERCHANT_SKEW) for m in merchant_ids)
    customer_ids = list(methods)
    customer_cum = ref.cumulative(ref.popularity(c, ref.CUSTOMER_SKEW) for c in customer_ids)
    cutoff = now - timedelta(minutes=2)

    rows, refund_intents = [], []
    day = window_start.astimezone(ref.IST).date()
    while day <= now.astimezone(ref.IST).date():
        factor = ref.WEEKEND_FACTOR if day.weekday() >= 5 else 1.0
        n = int(orders_per_day * factor * rng.uniform(0.9, 1.1))
        hours = rng.choices(range(24), weights=ref.HOURLY_WEIGHTS, k=n)
        merchants = rng.choices(merchant_ids, cum_weights=merchant_cum, k=n)
        customers = rng.choices(customer_ids, cum_weights=customer_cum, k=n)
        midnight = datetime(day.year, day.month, day.day, tzinfo=ref.IST)

        for hour, merchant_id, customer_id in zip(hours, merchants, customers):
            t = midnight + timedelta(hours=hour, seconds=rng.uniform(0, 3600))
            if not window_start <= t <= cutoff:
                continue
            category = merchant_category[merchant_id]
            amount = ref.amount_paise(rng, category)
            order_id = "order_" + ref.new_id(rng)[:16]
            customer_methods = methods[customer_id]
            method_id, method_type = ref.choose_method(rng, customer_methods)

            for attempt in range(1, ref.MAX_ATTEMPTS + 1):
                key = ref.new_id(rng)
                reason = ref.payment_failure(rng, method_type)
                if reason:
                    failed_at = t + timedelta(seconds=rng.uniform(1, 5))
                    rows.append([merchant_id, customer_id, method_id, order_id, key, amount,
                                 "failed", reason, t, None, None, failed_at])
                    if not ref.should_retry(rng, attempt):
                        break
                    t = failed_at + timedelta(seconds=rng.uniform(10, 120))
                    if len(customer_methods) > 1 and rng.random() < ref.SWITCH_METHOD_PROBABILITY:
                        others = [m for m in customer_methods if m[0] != method_id]
                        method_id, method_type = ref.choose_method(rng, others)
                    continue

                authorized = t + timedelta(seconds=rng.uniform(0.5, 2))
                captured = authorized + timedelta(seconds=rng.uniform(1, 4))
                rows.append([merchant_id, customer_id, method_id, order_id, key, amount,
                             "captured", None, t, authorized, captured, captured])
                if rng.random() < ref.CATEGORIES[category].refund_rate:
                    intent = _refund_intent(rng, merchant_id, key, amount, captured, now)
                    if intent:
                        refund_intents.append(intent)
                break
        day += timedelta(days=1)

    # Insert in time order, so payment_id increases with created_at like a real system.
    rows.sort(key=lambda r: r[PAYMENT_COLS.index("created_at")])
    _copy(conn, "payments", PAYMENT_COLS, rows)
    return refund_intents


def _refund_intent(rng, merchant_id, idempotency_key, amount, captured, now):
    created = captured + timedelta(hours=rng.uniform(1, 7 * 24))
    if created > now:
        return None
    processed = created + timedelta(minutes=rng.uniform(30, 48 * 60))
    if processed > now:
        status, updated = "pending", created
    else:
        status = "failed" if rng.random() < ref.REFUND_FAILURE_RATE else "processed"
        updated = processed
    return (merchant_id, idempotency_key,
            ref.refund_amount(rng, amount), status, ref.pick(rng, ref.REFUND_REASONS),
            created, updated)


def _load_refunds(conn, intents):
    # payment_id is generated by Postgres, so look ids up by the natural key we generated.
    lookup = dict(conn.execute(
        """
        SELECT p.idempotency_key, p.payment_id
        FROM payments p
        JOIN unnest(%s::bigint[], %s::text[]) AS k (merchant_id, idempotency_key)
          ON p.merchant_id = k.merchant_id AND p.idempotency_key = k.idempotency_key
        """,
        ([i[0] for i in intents], [i[1] for i in intents]),
    ).fetchall())
    rows = [[lookup[key], *rest] for _, key, *rest in intents]
    rows.sort(key=lambda r: r[REFUND_COLS.index("created_at")])
    _copy(conn, "refunds", REFUND_COLS, rows)
