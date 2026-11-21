#!/usr/bin/env bash
# Spark SQL against the lakehouse catalog, in a throwaway container.
#
#   ./spark/sql.sh                                         # interactive shell
#   ./spark/sql.sh -e "SELECT count(*) FROM bronze.payments"
#
# The catalog password is read from the container's environment, so it never
# appears in your shell history or the process list on the Mac.
set -euo pipefail
cd "$(dirname "$0")/.."

tty_flag=()
[ -t 0 ] || tty_flag=(-T)

exec docker compose run --rm --no-deps "${tty_flag[@]}" spark-tools bash -c \
    'exec /opt/spark/bin/spark-sql --conf spark.sql.catalog.lakehouse.jdbc.password="$ICEBERG_CATALOG_PASSWORD" "$@"' \
    spark-sql "$@"
