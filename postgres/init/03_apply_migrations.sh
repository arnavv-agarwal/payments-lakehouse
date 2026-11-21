#!/usr/bin/env bash
# Applies the versioned migrations in postgres/migrations (mounted at /migrations)
# in filename order, so a fresh database ends up with the same schema as one
# that was migrated over time. Runs once, on the first start of an empty volume.
# (A production setup would track applied versions with Flyway, Liquibase or Alembic.)
set -euo pipefail

for migration in /migrations/*.sql; do
    [ -e "$migration" ] || continue
    echo "applying ${migration##*/}"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f "$migration"
done
