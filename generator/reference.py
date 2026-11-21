"""Reference data and random helpers shared by the seed and live modes.

The numbers are rough and only meant to look plausible for Indian online
payments. They are not taken from any company's real data.
"""
import math
import random
import re
import uuid
from collections import namedtuple
from datetime import timedelta, timezone
from itertools import accumulate

IST = timezone(timedelta(hours=5, minutes=30), "IST")

# (city, state, weight)
CITIES = [
    ("Mumbai", "Maharashtra", 12), ("Delhi", "Delhi", 12), ("Bengaluru", "Karnataka", 11),
    ("Hyderabad", "Telangana", 8), ("Chennai", "Tamil Nadu", 7), ("Kolkata", "West Bengal", 6),
    ("Pune", "Maharashtra", 6), ("Ahmedabad", "Gujarat", 5), ("Gurugram", "Haryana", 4),
    ("Jaipur", "Rajasthan", 4), ("Noida", "Uttar Pradesh", 3), ("Lucknow", "Uttar Pradesh", 3),
    ("Surat", "Gujarat", 3), ("Kochi", "Kerala", 3), ("Chandigarh", "Chandigarh", 2),
    ("Indore", "Madhya Pradesh", 2), ("Bhubaneswar", "Odisha", 2), ("Guwahati", "Assam", 2),
    ("Coimbatore", "Tamil Nadu", 2), ("Nagpur", "Maharashtra", 2),
]

# median_inr and spread define a log-normal ticket size: most payments sit near
# the median, with a long tail of large ones (a few expensive flights or laptops).
Category = namedtuple("Category", "weight median_inr spread refund_rate name_suffixes")
CATEGORIES = {
    "ecommerce":     Category(25, 1200, 0.9, 0.06, ["Mart", "Store", "Bazaar", "Retail"]),
    "food_delivery": Category(18,  350, 0.6, 0.03, ["Foods", "Kitchen", "Cafe", "Eats"]),
    "grocery":       Category(12,  650, 0.7, 0.03, ["Fresh", "Grocers", "Supermart"]),
    "utilities":     Category(10, 1400, 0.7, 0.01, ["Power", "Broadband", "Gas"]),
    "education":     Category(8,  3000, 1.0, 0.02, ["Academy", "Classes", "Learning"]),
    "healthcare":    Category(8,   800, 0.9, 0.02, ["Pharmacy", "Clinic", "Diagnostics"]),
    "travel":        Category(7,  5500, 0.9, 0.07, ["Travels", "Holidays", "Tours"]),
    "fashion":       Category(6,  1800, 0.8, 0.08, ["Fashion", "Apparel", "Boutique"]),
    "gaming":        Category(6,   199, 0.8, 0.01, ["Games", "Play", "Arena"]),
}
MIN_TICKET_INR, MAX_TICKET_INR = 10, 200_000

PLANS = {"standard": 75, "growth": 20, "enterprise": 5}

METHOD_TYPES = {"upi": 68, "card": 17, "wallet": 9, "netbanking": 6}
UPI_APPS = {"PhonePe": 47, "Google Pay": 36, "Paytm": 7, "CRED": 3, "Amazon Pay": 3, "BHIM": 2}
BANKS = {"SBI": 22, "HDFC": 20, "ICICI": 18, "Axis": 12, "Kotak": 8, "Bank of Baroda": 6,
         "PNB": 5, "Yes Bank": 3, "IndusInd": 3, "IDFC First": 3}
WALLETS = {"Paytm": 40, "Amazon Pay": 30, "MobiKwik": 15, "PhonePe": 10, "Freecharge": 5}
CARD_NETWORKS = {"visa": 42, "rupay": 30, "mastercard": 25, "amex": 3}
METHODS_PER_CUSTOMER = {1: 55, 2: 35, 3: 10}

FAILURE_RATE = {"upi": 0.09, "card": 0.13, "netbanking": 0.16, "wallet": 0.05}
FAILURE_REASONS = {
    "upi": {"upi_pin_incorrect": 25, "insufficient_funds": 20, "transaction_timeout": 20,
            "user_declined": 20, "bank_server_down": 15},
    "card": {"authentication_failed": 30, "insufficient_funds": 25, "card_declined": 20,
             "bank_server_down": 15, "incorrect_cvv": 10},
    "netbanking": {"user_cancelled": 45, "bank_server_down": 30, "session_timeout": 25},
    "wallet": {"insufficient_balance": 60, "user_cancelled": 40},
}
RETRY_PROBABILITY = 0.55          # after a failure, the customer tries again
MAX_ATTEMPTS = 3                  # per order
SWITCH_METHOD_PROBABILITY = 0.40  # a retry uses a different saved method

PARTIAL_REFUND_PROBABILITY = 0.35
REFUND_FAILURE_RATE = 0.04
REFUND_REASONS = {"customer_request": 40, "order_cancelled": 25, "item_out_of_stock": 15,
                  "service_not_delivered": 12, "duplicate_payment": 8}

# Relative order volume by hour of day in IST (index 0 = midnight to 1 am).
HOURLY_WEIGHTS = [3, 2, 1, 1, 1, 1, 2, 3, 5, 6, 7, 7, 8, 8, 7, 7, 7, 8, 9, 10, 11, 11, 9, 6]
WEEKEND_FACTOR = 1.15

# Reserved for documentation and testing (RFC 2606), so no real inbox gets mail.
EMAIL_DOMAINS = ["example.com", "example.org", "example.net"]

# Pareto shape for traffic skew. Lower = more skewed (1.16 is roughly the 80/20 rule).
MERCHANT_SKEW, CUSTOMER_SKEW = 1.16, 2.0


def pick(rng, weights):
    """One key from a {value: weight} dict."""
    return rng.choices(list(weights), weights=list(weights.values()))[0]


def cumulative(weights):
    """Cumulative weights, so rng.choices(..., cum_weights=...) is a binary search per draw."""
    return list(accumulate(weights))


def popularity(entity_id, skew):
    """A stable heavy-tailed weight per id: a few merchants get most of the traffic.

    Seeded by the id itself, so the same merchant is equally popular across restarts.
    """
    return min(random.Random(entity_id).paretovariate(skew), 200.0)


def new_id(rng):
    """A random 32-char hex id drawn from rng (reproducible when rng is seeded)."""
    return uuid.UUID(int=rng.getrandbits(128), version=4).hex


def pick_city(rng):
    city, state, _ = rng.choices(CITIES, weights=[c[2] for c in CITIES])[0]
    return city, state


def mdr_for_plan(rng, plan):
    """Fee rate in basis points. Enterprise rates are negotiated per merchant."""
    return {"standard": 200, "growth": 180}.get(plan) or rng.randint(140, 170)


def merchant_row(rng, fake, plan=None):
    category = pick(rng, {name: c.weight for name, c in CATEGORIES.items()})
    plan = plan or pick(rng, PLANS)
    city, state = pick_city(rng)
    return {
        "merchant_name": f"{fake.last_name()} {rng.choice(CATEGORIES[category].name_suffixes)}",
        "category": category, "city": city, "state": state,
        "pricing_plan": plan, "mdr_bps": mdr_for_plan(rng, plan),
    }


def customer_row(rng, fake, kyc_status=None):
    first, last = fake.first_name(), fake.last_name()
    local = re.sub(r"[^a-z]", "", first.lower()) + "." + re.sub(r"[^a-z]", "", last.lower())
    city, state = pick_city(rng)
    return {
        "full_name": f"{first} {last}",
        # The random suffix keeps emails unique even when two people share a name.
        "email": f"{local}.{rng.getrandbits(32):08x}@{rng.choice(EMAIL_DOMAINS)}",
        "phone": "+91" + rng.choice("6789") + "".join(rng.choices("0123456789", k=9)),
        "city": city, "state": state,
        "kyc_status": kyc_status or pick(rng, {"verified": 80, "pending": 17, "rejected": 3}),
    }


def method_row(rng, customer_id, method_type=None):
    method_type = method_type or pick(rng, METHOD_TYPES)
    providers = {"upi": UPI_APPS, "wallet": WALLETS}.get(method_type, BANKS)
    is_card = method_type == "card"
    return {
        "customer_id": customer_id, "method_type": method_type, "provider": pick(rng, providers),
        "card_network": pick(rng, CARD_NETWORKS) if is_card else None,
        "card_last4": f"{rng.randrange(10_000):04d}" if is_card else None,
    }


def choose_method(rng, methods):
    """Pick one of a customer's (method_id, method_type) pairs, favouring UPI."""
    return rng.choices(methods, weights=[METHOD_TYPES[m[1]] for m in methods])[0]


def amount_paise(rng, category):
    c = CATEGORIES[category]
    rupees = round(c.median_inr * math.exp(c.spread * rng.gauss(0, 1)))
    return min(max(rupees, MIN_TICKET_INR), MAX_TICKET_INR) * 100


def payment_failure(rng, method_type):
    """None if the attempt succeeds, otherwise a failure reason."""
    if rng.random() >= FAILURE_RATE[method_type]:
        return None
    return pick(rng, FAILURE_REASONS[method_type])


def should_retry(rng, attempt):
    return attempt < MAX_ATTEMPTS and rng.random() < RETRY_PROBABILITY


def refund_amount(rng, amount):
    if rng.random() < PARTIAL_REFUND_PROBABILITY:
        return max(100, int(amount * rng.uniform(0.1, 0.9)) // 100 * 100)
    return amount
