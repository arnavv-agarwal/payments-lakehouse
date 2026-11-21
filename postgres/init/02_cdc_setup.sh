#!/usr/bin/env bash
# Prepares Postgres for change data capture (CDC) with Debezium:
#   1. a least-privilege login role that can stream the WAL and read tables
#      (SELECT is needed for Debezium's initial snapshot), and
#   2. a publication listing exactly which tables get streamed.
# Runs once, after 01_schema.sql, on the first start of an empty data volume.
set -euo pipefail

psql -v ON_ERROR_STOP=1 \
     -v dbz_password="$DEBEZIUM_PASSWORD" \
     -v db="$POSTGRES_DB" \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'EOSQL'
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD :'dbz_password';

GRANT CONNECT ON DATABASE :"db" TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

-- Created here by the table owner, so Debezium never needs ownership rights.
-- The connector will be configured with publication.autocreate.mode=disabled.
CREATE PUBLICATION dbz_payments
    FOR TABLE merchants, customers, payment_methods, payments, refunds;
EOSQL
