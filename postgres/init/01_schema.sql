-- Source OLTP schema for a simplified Indian payment gateway.
-- Runs once, on the first start of an empty data volume.
--
-- Conventions:
--   * Money is BIGINT paise (1 INR = 100 paise). Never FLOAT for money.
--   * Every timestamp is TIMESTAMPTZ (stored as UTC).
--   * Every table has created_at and updated_at; updated_at is maintained by a trigger.
--   * Status columns are TEXT + CHECK instead of Postgres ENUM types, so adding a
--     value later is a simple constraint change, not an ALTER TYPE.

BEGIN;

-- Keeps updated_at current on every UPDATE. If the caller sets updated_at
-- explicitly (e.g. when backfilling history), that value is kept.
CREATE FUNCTION set_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.updated_at IS NOT DISTINCT FROM OLD.updated_at THEN
        NEW.updated_at := now();
    END IF;
    RETURN NEW;
END;
$$;

-- Businesses that accept payments. pricing_plan and mdr_bps change over time,
-- which is what the SCD Type 2 merchant dimension will track later.
CREATE TABLE merchants (
    merchant_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    merchant_name  TEXT        NOT NULL,
    category       TEXT        NOT NULL,
    city           TEXT        NOT NULL,
    state          TEXT        NOT NULL,
    pricing_plan   TEXT        NOT NULL DEFAULT 'standard'
                   CHECK (pricing_plan IN ('standard', 'growth', 'enterprise')),
    mdr_bps        INTEGER     NOT NULL CHECK (mdr_bps BETWEEN 0 AND 500),
    status         TEXT        NOT NULL DEFAULT 'active'
                   CHECK (status IN ('active', 'suspended', 'closed')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON COLUMN merchants.mdr_bps IS
    'Merchant discount rate in basis points (200 = 2.00% of the payment amount).';

-- People who pay. email and phone are PII and get masked or hashed in the lakehouse.
CREATE TABLE customers (
    customer_id  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    full_name    TEXT        NOT NULL,
    email        TEXT        NOT NULL UNIQUE,
    phone        TEXT        NOT NULL,
    city         TEXT        NOT NULL,
    state        TEXT        NOT NULL,
    kyc_status   TEXT        NOT NULL DEFAULT 'pending'
                 CHECK (kyc_status IN ('pending', 'verified', 'rejected')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A customer's saved instruments: UPI, card, netbanking, wallet.
CREATE TABLE payment_methods (
    payment_method_id  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id        BIGINT      NOT NULL REFERENCES customers (customer_id),
    method_type        TEXT        NOT NULL
                       CHECK (method_type IN ('upi', 'card', 'netbanking', 'wallet')),
    provider           TEXT        NOT NULL,  -- bank or app, e.g. HDFC, SBI, PhonePe
    card_network       TEXT        CHECK (card_network IN ('visa', 'mastercard', 'rupay', 'amex')),
    card_last4         CHAR(4),
    is_active          BOOLEAN     NOT NULL DEFAULT true,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT card_fields_only_for_cards CHECK (
        (method_type = 'card'  AND card_network IS NOT NULL AND card_last4 IS NOT NULL)
     OR (method_type <> 'card' AND card_network IS NULL     AND card_last4 IS NULL)
    ),
    -- Lets payments reference (method, customer) together, see fk below.
    CONSTRAINT uq_method_customer UNIQUE (payment_method_id, customer_id)
);

-- One row per payment attempt. An order can have several attempts
-- (e.g. a failed UPI attempt, then a successful card attempt).
-- Lifecycle: created -> authorized -> captured, or created -> failed.
CREATE TABLE payments (
    payment_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    merchant_id        BIGINT      NOT NULL REFERENCES merchants (merchant_id),
    customer_id        BIGINT      NOT NULL REFERENCES customers (customer_id),
    payment_method_id  BIGINT      NOT NULL,
    merchant_order_id  TEXT        NOT NULL,
    idempotency_key    TEXT        NOT NULL,
    amount_paise       BIGINT      NOT NULL CHECK (amount_paise > 0),
    currency           CHAR(3)     NOT NULL DEFAULT 'INR',
    status             TEXT        NOT NULL DEFAULT 'created'
                       CHECK (status IN ('created', 'authorized', 'captured', 'failed')),
    failure_reason     TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    authorized_at      TIMESTAMPTZ,
    captured_at        TIMESTAMPTZ,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- The payment method must belong to the paying customer.
    CONSTRAINT fk_method_belongs_to_customer
        FOREIGN KEY (payment_method_id, customer_id)
        REFERENCES payment_methods (payment_method_id, customer_id),
    -- A retried API call with the same key must not create a second payment.
    CONSTRAINT uq_merchant_idempotency_key UNIQUE (merchant_id, idempotency_key),
    CONSTRAINT failure_reason_iff_failed CHECK ((status = 'failed') = (failure_reason IS NOT NULL))
);

-- Partial or full refunds against a captured payment.
CREATE TABLE refunds (
    refund_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_id    BIGINT      NOT NULL REFERENCES payments (payment_id),
    amount_paise  BIGINT      NOT NULL CHECK (amount_paise > 0),
    status        TEXT        NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'processed', 'failed')),
    reason        TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Postgres does not index foreign keys automatically.
CREATE INDEX idx_payment_methods_customer ON payment_methods (customer_id);
CREATE INDEX idx_payments_merchant_created ON payments (merchant_id, created_at);
CREATE INDEX idx_payments_customer        ON payments (customer_id);
CREATE INDEX idx_payments_order           ON payments (merchant_id, merchant_order_id);
CREATE INDEX idx_refunds_payment          ON refunds (payment_id);

CREATE TRIGGER trg_merchants_updated_at       BEFORE UPDATE ON merchants
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_customers_updated_at       BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_payment_methods_updated_at BEFORE UPDATE ON payment_methods
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_payments_updated_at        BEFORE UPDATE ON payments
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_refunds_updated_at         BEFORE UPDATE ON refunds
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
