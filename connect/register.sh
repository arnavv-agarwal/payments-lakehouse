#!/usr/bin/env bash
# Creates or updates the Debezium connector through the Kafka Connect REST API.
#
# PUT /connectors/<name>/config is idempotent: it creates the connector the
# first time and updates its config on every later run, so this script is
# safe to re-run after editing the JSON file.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
NAME="payments-postgres-cdc"
CONFIG="$(cd "$(dirname "$0")" && pwd)/${NAME}.json"

echo "Waiting for Kafka Connect at ${CONNECT_URL} to load the Debezium plugin..."
for _ in $(seq 1 60); do
    if curl -fsS "${CONNECT_URL}/connector-plugins" 2>/dev/null | grep -q PostgresConnector; then
        break
    fi
    sleep 2
done

curl -sS --fail-with-body -X PUT -H "Content-Type: application/json" \
     --data @"${CONFIG}" "${CONNECT_URL}/connectors/${NAME}/config" > /dev/null
echo "Connector ${NAME} registered."

sleep 5
curl -sS "${CONNECT_URL}/connectors/${NAME}/status"
echo
