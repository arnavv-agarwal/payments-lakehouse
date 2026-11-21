"""`run`: a live stream of payment activity, one small transaction per state change.

A payment moves through its lifecycle in separate transactions a few seconds
apart (inserted as created, then authorized, then captured), so a CDC pipeline
sees several change events per payment, as it would in production. Reference
tables change occasionally too: signups, KYC decisions, merchant plan changes.
"""
import heapq
import itertools
import logging
import math
import random
import signal
import time
from collections import Counter, defaultdict
from dataclasses import dataclass

import psycopg
from faker import Faker

from . import reference as ref

log = logging.getLogger(__name__)

REPORT_EVERY_S = 10
DUPLICATE_REQUEST_PROBABILITY = 0.005  # merchant's HTTP client times out and re-sends
MAX_ACTIVE_METHODS = 4
MIN_ACTIVE_MERCHANTS = 10

# Relative frequency of reference-data changes.
DIMENSION_EVENTS = {
    "customer_signup": 25, "customer_kyc_decision": 20, "method_added": 15,
    "customer_moved_city": 10, "method_removed": 10, "merchant_plan_change": 10,
    "merchant_status_change": 5, "merchant_signup": 5,
}
# Next plan for a merchant on each plan (enterprise -> enterprise = a renegotiated rate).
PLAN_TRANSITIONS = {
    "standard": {"growth": 80, "enterprise": 20},
    "growth": {"enterprise": 60, "standard": 40},
    "enterprise": {"enterprise": 50, "growth": 50},
}

INSERT_PAYMENT = """
    INSERT INTO payments (merchant_id, customer_id, payment_method_id, merchant_order_id,
                          idempotency_key, amount_paise)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (merchant_id, idempotency_key) DO NOTHING
    RETURNING payment_id
"""
INSERT_METHOD = """
    INSERT INTO payment_methods (customer_id, method_type, provider, card_network, card_last4)
    VALUES (%(customer_id)s, %(method_type)s, %(provider)s, %(card_network)s, %(card_last4)s)
    RETURNING payment_method_id
"""


@dataclass(frozen=True)
class Payment:
    payment_id: int
    merchant_id: int
    customer_id: int
    method_id: int
    method_type: str
    order_id: str
    amount: int
    attempt: int


class Simulator:
    def __init__(self, conn, *, rate, dim_rate):
        self.conn = conn
        self.rate = rate
        self.dim_rate = dim_rate
        self.rng = random.Random()  # unseeded on purpose: ids must not repeat across runs
        self.fake = Faker("en_IN")
        self.queue = []              # heap of (due_time, tiebreak, function, args)
        self.tiebreak = itertools.count()
        self.stats = Counter()
        self.running = True

    # ------------------------------------------------------------------ main loop

    def run(self, duration=None):
        signal.signal(signal.SIGTERM, lambda *_: self.stop())
        self._load_state()
        self._recover_in_flight()

        start = time.monotonic()
        next_order = start + self.rng.expovariate(self.rate)
        next_dim = start + self.rng.expovariate(self.dim_rate) if self.dim_rate > 0 else math.inf
        next_report = start + REPORT_EVERY_S
        log.info("streaming: %.2f orders/s, %.2f reference changes/s (Ctrl+C to stop)",
                 self.rate, self.dim_rate)
        try:
            while self.running:
                now = time.monotonic()
                if duration and now - start >= duration:
                    break
                if self.queue and self.queue[0][0] <= now:
                    _, _, fn, args = heapq.heappop(self.queue)
                    self._safely(fn, *args)
                elif now >= next_order:
                    self._safely(self.new_order)
                    # Poisson arrivals: exponential gaps between orders. After a stall
                    # (laptop asleep, DB slow), resume instead of bursting to catch up.
                    next_order = max(next_order + self.rng.expovariate(self.rate), now - 1)
                elif now >= next_dim:
                    self._safely(self.dimension_change)
                    next_dim = max(next_dim + self.rng.expovariate(self.dim_rate), now - 1)
                elif now >= next_report:
                    self._report(now - start)
                    next_report += REPORT_EVERY_S
                else:
                    due = min(next_order, next_dim, next_report,
                              self.queue[0][0] if self.queue else math.inf)
                    time.sleep(min(max(due - now, 0), 0.5))
        except KeyboardInterrupt:
            pass
        self._report(time.monotonic() - start, final=True)

    def stop(self):
        self.running = False

    def schedule(self, delay_s, fn, *args):
        heapq.heappush(self.queue, (time.monotonic() + delay_s, next(self.tiebreak), fn, args))

    def _safely(self, fn, *args):
        """A constraint violation is logged and counted, not fatal. Anything else is."""
        try:
            fn(*args)
        except psycopg.IntegrityError as e:
            self.stats["errors"] += 1
            log.warning("%s failed: %s", fn.__name__, str(e).splitlines()[0])

    # ------------------------------------------------------------------ in-memory state

    def _load_state(self):
        rows = self.conn.execute("SELECT merchant_id, category, status FROM merchants").fetchall()
        self.merchants = {mid: {"category": cat, "status": st} for mid, cat, st in rows}
        self._refresh_merchant_weights()

        self.customer_ids, self.customer_cum = [], []
        for (customer_id,) in self.conn.execute("SELECT customer_id FROM customers ORDER BY 1"):
            self._add_customer(customer_id)

        self.methods = defaultdict(list)  # customer_id -> [(method_id, method_type)], active only
        for method_id, customer_id, method_type in self.conn.execute(
                "SELECT payment_method_id, customer_id, method_type FROM payment_methods "
                "WHERE is_active"):
            self.methods[customer_id].append((method_id, method_type))

        if not self.active_merchants or not self.customer_ids:
            raise SystemExit("No merchants or customers found. Run `python -m generator seed` first.")
        log.info("loaded %d merchants, %d customers", len(self.merchants), len(self.customer_ids))

    def _refresh_merchant_weights(self):
        self.active_merchants = [m for m, v in self.merchants.items() if v["status"] == "active"]
        self.merchant_cum = ref.cumulative(
            ref.popularity(m, ref.MERCHANT_SKEW) for m in self.active_merchants)

    def _add_customer(self, customer_id):
        previous = self.customer_cum[-1] if self.customer_cum else 0.0
        self.customer_ids.append(customer_id)
        self.customer_cum.append(previous + ref.popularity(customer_id, ref.CUSTOMER_SKEW))

    def _recover_in_flight(self):
        """Finish payments and refunds a previous run left mid-lifecycle."""
        payments = self.conn.execute("""
            SELECT p.payment_id, p.status, p.merchant_id, p.customer_id, p.payment_method_id,
                   pm.method_type, p.merchant_order_id, p.amount_paise
            FROM payments p JOIN payment_methods pm USING (payment_method_id)
            WHERE p.status IN ('created', 'authorized')""").fetchall()
        for pid, status, mid, cid, method_id, method_type, order_id, amount in payments:
            p = Payment(pid, mid, cid, method_id, method_type, order_id, amount,
                        attempt=ref.MAX_ATTEMPTS)  # unknown attempt number: don't retry
            step = self.resolve if status == "created" else self.capture
            self.schedule(self.rng.uniform(0.5, 3), step, p)

        refunds = self.conn.execute("SELECT refund_id FROM refunds WHERE status = 'pending'").fetchall()
        for (refund_id,) in refunds:
            self.schedule(self.rng.uniform(1, 10), self.resolve_refund, refund_id)
        log.info("recovered %d in-flight payments and %d pending refunds",
                 len(payments), len(refunds))

    # ------------------------------------------------------------------ payment lifecycle

    def new_order(self):
        merchant_id = self.rng.choices(self.active_merchants, cum_weights=self.merchant_cum)[0]
        customer_id = self.rng.choices(self.customer_ids, cum_weights=self.customer_cum)[0]
        if not self.methods[customer_id]:
            return
        method = ref.choose_method(self.rng, self.methods[customer_id])
        amount = ref.amount_paise(self.rng, self.merchants[merchant_id]["category"])
        order_id = "order_" + ref.new_id(self.rng)[:16]
        self.attempt(merchant_id, customer_id, method, order_id, amount, attempt=1)

    def attempt(self, merchant_id, customer_id, method, order_id, amount, attempt):
        params = (merchant_id, customer_id, method[0], order_id, ref.new_id(self.rng), amount)
        payment_id = self.conn.execute(INSERT_PAYMENT, params).fetchone()[0]
        self.stats["payments_created"] += 1
        if attempt > 1:
            self.stats["retries"] += 1

        if self.rng.random() < DUPLICATE_REQUEST_PROBABILITY:
            # Same request, same idempotency key: the unique constraint absorbs it.
            if self.conn.execute(INSERT_PAYMENT, params).fetchone() is None:
                self.stats["duplicate_requests_absorbed"] += 1

        p = Payment(payment_id, merchant_id, customer_id, method[0], method[1],
                    order_id, amount, attempt)
        self.schedule(self.rng.uniform(1, 4), self.resolve, p)

    def resolve(self, p):
        """created -> authorized, or created -> failed (and maybe a retry)."""
        reason = ref.payment_failure(self.rng, p.method_type)
        if reason is None:
            # The status guard in WHERE makes each transition apply at most once.
            self.conn.execute(
                "UPDATE payments SET status = 'authorized', authorized_at = now() "
                "WHERE payment_id = %s AND status = 'created'", (p.payment_id,))
            self.schedule(self.rng.uniform(1, 4), self.capture, p)
            return

        self.conn.execute(
            "UPDATE payments SET status = 'failed', failure_reason = %s "
            "WHERE payment_id = %s AND status = 'created'", (reason, p.payment_id))
        self.stats["failed"] += 1
        if ref.should_retry(self.rng, p.attempt):
            self.schedule(self.rng.uniform(3, 15), self.retry, p)

    def retry(self, p):
        if self.merchants[p.merchant_id]["status"] != "active":
            return
        methods = self.methods[p.customer_id]
        current = (p.method_id, p.method_type)
        others = [m for m in methods if m != current]
        if others and (current not in methods or self.rng.random() < ref.SWITCH_METHOD_PROBABILITY):
            method = ref.choose_method(self.rng, others)
        elif current in methods:
            method = current
        else:
            return  # the method was removed and there is no other one
        self.attempt(p.merchant_id, p.customer_id, method, p.order_id, p.amount, p.attempt + 1)

    def capture(self, p):
        self.conn.execute(
            "UPDATE payments SET status = 'captured', captured_at = now() "
            "WHERE payment_id = %s AND status = 'authorized'", (p.payment_id,))
        self.stats["captured"] += 1
        category = self.merchants[p.merchant_id]["category"]
        if self.rng.random() < ref.CATEGORIES[category].refund_rate:
            # Real refunds come hours or days later; compressed to minutes here.
            self.schedule(self.rng.uniform(30, 300), self.refund, p)

    def refund(self, p):
        refund_id = self.conn.execute(
            "INSERT INTO refunds (payment_id, amount_paise, reason) VALUES (%s, %s, %s) "
            "RETURNING refund_id",
            (p.payment_id, ref.refund_amount(self.rng, p.amount),
             ref.pick(self.rng, ref.REFUND_REASONS))).fetchone()[0]
        self.stats["refunds"] += 1
        self.schedule(self.rng.uniform(5, 30), self.resolve_refund, refund_id)

    def resolve_refund(self, refund_id):
        status = "failed" if self.rng.random() < ref.REFUND_FAILURE_RATE else "processed"
        self.conn.execute("UPDATE refunds SET status = %s WHERE refund_id = %s AND status = 'pending'",
                          (status, refund_id))

    # ------------------------------------------------------------------ reference data changes

    def dimension_change(self):
        event = ref.pick(self.rng, DIMENSION_EVENTS)
        if getattr(self, "_" + event)():
            self.stats[event] += 1
            self.stats["reference_changes"] += 1

    def _random_customer(self):
        return self.rng.choice(self.customer_ids)

    def _customer_signup(self):
        c = ref.customer_row(self.rng, self.fake, kyc_status="pending")
        # Customer and first payment method in one transaction: CDC will emit both
        # events with the same transaction id, on two different topics.
        with self.conn.transaction():
            row = self.conn.execute(
                "INSERT INTO customers (full_name, email, phone, city, state, kyc_status) "
                "VALUES (%(full_name)s, %(email)s, %(phone)s, %(city)s, %(state)s, %(kyc_status)s) "
                "ON CONFLICT (email) DO NOTHING RETURNING customer_id", c).fetchone()
            if row is None:
                return False
            m = ref.method_row(self.rng, row[0])
            method_id = self.conn.execute(INSERT_METHOD, m).fetchone()[0]
        self._add_customer(row[0])
        self.methods[row[0]].append((method_id, m["method_type"]))
        return True

    def _customer_kyc_decision(self):
        status = "verified" if self.rng.random() < 0.9 else "rejected"
        cur = self.conn.execute("""
            UPDATE customers SET kyc_status = %s
            WHERE customer_id = (SELECT customer_id FROM customers WHERE kyc_status = 'pending'
                                 ORDER BY random() LIMIT 1)""", (status,))
        return cur.rowcount == 1

    def _customer_moved_city(self):
        city, state = ref.pick_city(self.rng)
        cur = self.conn.execute(
            "UPDATE customers SET city = %s, state = %s WHERE customer_id = %s AND city <> %s",
            (city, state, self._random_customer(), city))
        return cur.rowcount == 1

    def _method_added(self):
        customer_id = self._random_customer()
        if len(self.methods[customer_id]) >= MAX_ACTIVE_METHODS:
            return False
        m = ref.method_row(self.rng, customer_id)
        method_id = self.conn.execute(INSERT_METHOD, m).fetchone()[0]
        self.methods[customer_id].append((method_id, m["method_type"]))
        return True

    def _method_removed(self):
        """A never-used method is deleted; a used one is soft-deleted (kept for history)."""
        sample = self.rng.sample(self.customer_ids, min(20, len(self.customer_ids)))
        candidates = [c for c in sample if len(self.methods[c]) > 1]
        if not candidates:
            return False
        customer_id = candidates[0]
        method = self.rng.choice(self.methods[customer_id])
        # Matching customer_id too lets Postgres use idx_payments_customer (a customer
        # has a few dozen payments) instead of scanning the whole payments table.
        deleted = self.conn.execute("""
            DELETE FROM payment_methods pm
            WHERE pm.payment_method_id = %s
              AND NOT EXISTS (SELECT 1 FROM payments p
                              WHERE p.customer_id = pm.customer_id
                                AND p.payment_method_id = pm.payment_method_id)""",
            (method[0],)).rowcount
        if not deleted:
            self.conn.execute(
                "UPDATE payment_methods SET is_active = false WHERE payment_method_id = %s",
                (method[0],))
        self.stats["methods_hard_deleted" if deleted else "methods_deactivated"] += 1
        self.methods[customer_id].remove(method)
        return True

    def _merchant_plan_change(self):
        merchant_id = self.rng.choice(self.active_merchants)
        (plan,) = self.conn.execute(
            "SELECT pricing_plan FROM merchants WHERE merchant_id = %s", (merchant_id,)).fetchone()
        new_plan = ref.pick(self.rng, PLAN_TRANSITIONS[plan])
        self.conn.execute(
            "UPDATE merchants SET pricing_plan = %s, mdr_bps = %s WHERE merchant_id = %s",
            (new_plan, ref.mdr_for_plan(self.rng, new_plan), merchant_id))
        return True

    def _merchant_status_change(self):
        suspended = [m for m, v in self.merchants.items() if v["status"] == "suspended"]
        if suspended and self.rng.random() < 0.5:
            merchant_id, new_status = self.rng.choice(suspended), "active"
        elif len(self.active_merchants) > MIN_ACTIVE_MERCHANTS:
            merchant_id, new_status = self.rng.choice(self.active_merchants), "suspended"
        else:
            return False
        self.conn.execute("UPDATE merchants SET status = %s WHERE merchant_id = %s",
                          (new_status, merchant_id))
        self.merchants[merchant_id]["status"] = new_status
        self._refresh_merchant_weights()
        return True

    def _merchant_signup(self):
        m = ref.merchant_row(self.rng, self.fake, plan="standard")
        merchant_id = self.conn.execute(
            "INSERT INTO merchants (merchant_name, category, city, state, pricing_plan, mdr_bps) "
            "VALUES (%(merchant_name)s, %(category)s, %(city)s, %(state)s, %(pricing_plan)s, "
            "%(mdr_bps)s) RETURNING merchant_id", m).fetchone()[0]
        self.merchants[merchant_id] = {"category": m["category"], "status": "active"}
        self._refresh_merchant_weights()
        return True

    # ------------------------------------------------------------------ reporting

    def _report(self, elapsed, final=False):
        s = self.stats
        log.info(
            "%s%5.0fs | payments %d (%.1f/s) captured %d failed %d retries %d | refunds %d | "
            "reference changes %d | duplicates absorbed %d | in flight %d | errors %d",
            "FINAL " if final else "", elapsed, s["payments_created"],
            s["payments_created"] / max(elapsed, 1e-9), s["captured"], s["failed"], s["retries"],
            s["refunds"], s["reference_changes"], s["duplicate_requests_absorbed"],
            len(self.queue), s["errors"])
        if final:
            changes = {k: v for k, v in s.items() if k in DIMENSION_EVENTS or k.startswith("methods_")}
            log.info("reference changes by type: %s", dict(sorted(changes.items())))
