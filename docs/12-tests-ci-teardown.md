# Phase 2, Step 12: Unit tests, CI, teardown and the README

## What this step adds

| File | Purpose |
|---|---|
| `tests/` | 23 pytest tests of the transformation logic, on a local SparkSession (no Kafka, Iceberg or AWS needed) |
| `requirements-dev.txt` | pyspark 4.1.4 (same as the Spark image), pytest 9.2.0, pyflakes 4.1.0 |
| `.github/workflows/ci.yml` | GitHub Actions on every push and pull request |
| `tools/teardown_aws.sh` | deletes every AWS resource of Phase 2 (`--plan` lists them first) |
| `README.md` | overview, architecture diagram, results, how to run |

## The tests

| File | What it pins down |
|---|---|
| `test_silver.py` | newest change wins by WAL position (LSN), not arrival order; a re-delivered event gives one row; a delete is soft and keeps the last known row; a delete with no earlier image keeps the key from `before`; Debezium's timestamp strings become timestamps |
| `test_gold.py` | SCD2: only Type 2 changes start a version, versions meet without gaps or overlaps, version 1 starts at creation, Type 1 name is current on every version, surrogate keys are stable across rebuilds, unknown merchants get an inferred member; a payment exactly at a plan change gets the new rate (half-open ranges); fees in integer paise rounded half up; attempt numbers per order; refunds count only when processed; the business day is the India date; the April-March financial year |
| `test_jobs_plumbing.py` | `--check` and `--check true` both work, nonsense values are rejected; unknown arguments fail locally but are ignored on Glue; `finish()` only exits on failure; secrets prefer the environment; the export takes rows above the per-partition bookmark |

**Time zones.** PySpark converts timestamps to Python datetimes in the
process's local time zone, so the same test gave different values on a
laptop in IST. `conftest.py` sets `TZ=UTC` before Spark starts.

**Do the tests catch real bugs?** Three bugs were injected into copies of
the jobs, one at a time:

| Injected bug | Caught by |
|---|---|
| fee rounded down instead of half up | `test_fee_is_integer_paise_rounded_half_up` |
| validity range closed at the start (`>` instead of `>=`) | `test_fee_uses_the_rate_in_force_at_payment_time_half_open` |
| silver orders by Kafka offset instead of LSN | `test_newest_change_wins_by_wal_position_not_arrival_order` |

Each bug failed exactly the test written for it; the other 22 passed.

## CI

Three jobs on `ubuntu-24.04`:

| Job | Steps | First run |
|---|---|---|
| Lint | pyflakes on all Python; ShellCheck (warning level) on all shell scripts; `docker compose config` with `.env.example` | 14 s |
| Unit tests | Java 21 (Temurin), Python 3.12, `pip install -r requirements-dev.txt`, `pytest` | 90 s (23 passed) |
| Terraform | `terraform fmt -check`, `init -backend=false`, `validate` | 18 s |

The workflow has read-only repository permissions and no cloud credentials:
nothing in CI can touch AWS.

## Teardown

`./tools/teardown_aws.sh --plan` lists what would go: the 31 Terraform
resources (bucket and all data, 6 Glue jobs, 4 databases and their tables,
2 IAM identities, Athena workgroup, 3 log groups, budget, job scripts) plus
the SSM parameter that Terraform doesn't manage. Before writing it, the
account was checked for anything created outside Terraform: only AWS's own
defaults (`primary` workgroup, the account's sign-in role) exist.

The real run asks for the bucket name as confirmation, deletes the SSM
parameter, runs `terraform destroy`, blanks the dead Airflow key in `.env`,
then checks that no bucket, Glue job, database or IAM identity is left.

## Before making the repository public

The whole history was scanned: none of the 11 secret values in `.env`, no
AWS account id, no access-key-shaped strings and no private keys appear in any
commit.
