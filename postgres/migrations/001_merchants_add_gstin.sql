-- 001: merchants get an optional GSTIN, India's 15-character GST identification
-- number (2-digit state code + PAN + entity number + 'Z' + check character).
--
-- Additive and nullable, so it is backward compatible for every consumer:
-- Debezium registers a new Avro schema version with gstin as an optional field,
-- and readers of the old version are unaffected.
ALTER TABLE merchants
    ADD COLUMN gstin TEXT CHECK (gstin ~ '^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][1-9A-Z]Z[0-9A-Z]$');
