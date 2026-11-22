# Payments lakehouse: real-time CDC to an Iceberg star schema, locally and on AWS

[![CI](https://github.com/arnavv-agarwal/payments-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/arnavv-agarwal/payments-lakehouse/actions/workflows/ci.yml)

A data platform for a simulated Indian payment gateway. Every change in the
payments database is captured within a second (Debezium CDC into Kafka),
streamed by PySpark into Apache Iceberg tables (bronze, silver), and modelled
into a star schema with an SCD Type 2 merchant dimension (gold), so fees are
always computed with the rate in force at payment time. Airflow orchestrates
the batch layer and 40 data-quality checks. The same Spark jobs then run on
**AWS Glue** over S3, the Glue Data Catalog and Athena, provisioned with
Terraform, and produce results identical to the local run.

Correctness is measured rather than assumed: column-by-column reconciliation
against the source, crash and replay tests, an outage drill, deliberately
corrupted rows, and unit tests checked against injected bugs.

## Architecture

```mermaid
flowchart LR
  subgraph local["Laptop: Docker Compose"]
    gen["Python generator<br/>(payments, refunds,<br/>plan changes)"] --> pg[("Postgres 17<br/>source OLTP")]
    pg -- "WAL, logical decoding" --> dbz["Debezium 3.7<br/>on Kafka Connect"]
    dbz -- "Avro + Schema Registry" --> kafka[("Kafka 8.4<br/>KRaft")]
    kafka --> bronze_job["Spark Structured Streaming<br/>exactly-once"] --> bronze[("Iceberg bronze<br/>every change event")]
    bronze --> silver_job["Spark streaming MERGE<br/>idempotent by LSN"] --> silver[("Iceberg silver<br/>current state, PII hashed")]
    silver --> gold_job["Spark batch<br/>star schema, SCD2"] --> gold[("Iceberg gold")]
    airflow["Airflow 3.3"] -. "schedules" .-> gold_job
    store[("RustFS (S3 API)<br/>+ JDBC catalog")]
  end

  subgraph aws["AWS ap-southeast-2: Terraform"]
    s3[("S3<br/>landing + warehouse")]
    glue["Glue 6.0 jobs (Spark 4.1)<br/>load, silver, gold, checks"]
    catalog[("Glue Data Catalog")]
    athena["Athena"]
  end

  bronze -- "export_bronze.py<br/>landing batches" --> s3
  airflow -. "GlueJobOperator" .-> glue
  s3 <--> glue
  glue --> catalog
  athena --> catalog
```

## Highlights

| Area | What was built | Evidence |
|---|---|---|
| **CDC** | Debezium on Postgres logical replication, Avro with Schema Registry | Connect killed with SIGKILL mid-stream: **0 rows lost**, 51 re-delivered (at-least-once), reconciliation PASS |
| **Streaming ingest** | PySpark Structured Streaming, Kafka to Iceberg, checkpointed | Driver killed + schema change mid-stream: **0 duplicates** across 664,930 rows |
| **Schema evolution** | A new source column (GSTIN) added during streaming | Job stops on an unknown schema id instead of mis-decoding, restarts, Iceberg table evolves |
| **Silver** | Newest event per key by WAL position (LSN), idempotent MERGE, soft deletes, HMAC-hashed PII | **737,259 rows match Postgres column by column**; full replay of all bronze: **+0 rows** |
| **Gold** | Kimball star schema, accumulating-snapshot fact, SCD Type 2 merchants, point-in-time joins, late-arriving dimensions | Fees credited 7x too high without SCD2 (₹11.0 lakh vs ₹1.57 lakh); incremental build equals a full rebuild |
| **Data quality** | 40 checks (25 on AWS): accuracy, completeness, freshness, keys, SCD2 rules, privacy | Outage drill: silver stopped 9 min, freshness check measured 526-541 s lag and failed the run |
| **Orchestration** | Airflow 3.3 (DockerOperator locally, GlueJobOperator on AWS) | A run with no new data skips all cloud tasks: $0 |
| **Cloud** | S3, Glue 6.0, Glue Data Catalog, Athena, IAM least privilege, SSM SecureString, Budgets, all in Terraform | AWS silver and gold **identical** to local (Spark vs Athena fingerprints, to the paisa) |
| **Testing and CI** | 23 unit tests (PySpark transformations, job plumbing), pyflakes, ShellCheck, Terraform validate, on GitHub Actions | Three injected bugs (fee rounding, validity boundary, LSN ordering) each caught by its test |

## Tech stack

| | |
|---|---|
| **Languages** | ![Python 3.12](https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white) ![SQL](https://img.shields.io/badge/SQL-336791?style=for-the-badge) ![Bash](https://img.shields.io/badge/Bash-4EAA25?style=for-the-badge&logo=gnubash&logoColor=white) |
| **Source and CDC** | ![PostgreSQL 17](https://img.shields.io/badge/PostgreSQL-17-4169E1?style=for-the-badge&logo=postgresql&logoColor=white) ![Debezium 3.7](https://img.shields.io/badge/Debezium-3.7-48A23F?style=for-the-badge) ![Apache Kafka 8.4 KRaft](https://img.shields.io/badge/Apache_Kafka-8.4_KRaft-231F20?style=for-the-badge&logo=apachekafka&logoColor=white) ![Kafka Connect](https://img.shields.io/badge/Kafka_Connect-231F20?style=for-the-badge&logo=apachekafka&logoColor=white) ![Schema Registry Confluent](https://img.shields.io/badge/Schema_Registry-Confluent-1C4E80?style=for-the-badge) ![Apache Avro](https://img.shields.io/badge/Apache_Avro-30638E?style=for-the-badge&logo=apacheavro&logoColor=white) ![Kafka UI kafbat](https://img.shields.io/badge/Kafka_UI-kafbat-555555?style=for-the-badge) |
| **Processing** | ![Apache Spark 4.1](https://img.shields.io/badge/Apache_Spark-4.1-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white) ![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white) ![Structured Streaming](https://img.shields.io/badge/Structured_Streaming-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white) ![Spark SQL](https://img.shields.io/badge/Spark_SQL-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white) |
| **Lakehouse storage** | ![Apache Iceberg 1.12](https://img.shields.io/badge/Apache_Iceberg-1.12-2D6DB5?style=for-the-badge) ![Apache Parquet](https://img.shields.io/badge/Apache_Parquet-50ABF1?style=for-the-badge&logo=apacheparquet&logoColor=white) ![RustFS S3 API](https://img.shields.io/badge/RustFS-S3_API-555555?style=for-the-badge) |
| **Orchestration** | ![Apache Airflow 3.3](https://img.shields.io/badge/Apache_Airflow-3.3-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white) |
| **AWS** | ![AWS S3](https://img.shields.io/badge/AWS-S3-FF9900?style=for-the-badge&labelColor=232F3E) ![AWS Glue 6.0](https://img.shields.io/badge/AWS-Glue_6.0-FF9900?style=for-the-badge&labelColor=232F3E) ![AWS Glue Data Catalog](https://img.shields.io/badge/AWS-Glue_Data_Catalog-FF9900?style=for-the-badge&labelColor=232F3E) ![AWS Athena](https://img.shields.io/badge/AWS-Athena-FF9900?style=for-the-badge&labelColor=232F3E) ![AWS IAM](https://img.shields.io/badge/AWS-IAM-FF9900?style=for-the-badge&labelColor=232F3E) ![AWS Systems Manager](https://img.shields.io/badge/AWS-Systems_Manager-FF9900?style=for-the-badge&labelColor=232F3E) ![AWS CloudWatch Logs](https://img.shields.io/badge/AWS-CloudWatch_Logs-FF9900?style=for-the-badge&labelColor=232F3E) ![AWS Budgets](https://img.shields.io/badge/AWS-Budgets-FF9900?style=for-the-badge&labelColor=232F3E) |
| **Infrastructure** | ![Terraform 1.17](https://img.shields.io/badge/Terraform-1.17-844FBA?style=for-the-badge&logo=terraform&logoColor=white) ![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white) ![Docker Compose](https://img.shields.io/badge/Docker_Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white) ![Colima](https://img.shields.io/badge/Colima-555555?style=for-the-badge) |
| **Testing and CI** | ![pytest 9.2](https://img.shields.io/badge/pytest-9.2-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white) ![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=githubactions&logoColor=white) ![ShellCheck](https://img.shields.io/badge/ShellCheck-555555?style=for-the-badge) ![pyflakes](https://img.shields.io/badge/pyflakes-3776AB?style=for-the-badge) |
| **Python libraries** | ![psycopg 3.3](https://img.shields.io/badge/psycopg-3.3-3776AB?style=for-the-badge) ![confluent-kafka 2.16](https://img.shields.io/badge/confluent--kafka-2.16-3776AB?style=for-the-badge) ![Faker 40](https://img.shields.io/badge/Faker-40-3776AB?style=for-the-badge) ![boto3](https://img.shields.io/badge/boto3-3776AB?style=for-the-badge) |

## Repository layout

| Path | Contents |
|---|---|
| `postgres/` | source schema (constraints, CDC publication), versioned migrations |
| `generator/` | realistic synthetic traffic: seed history, then live payments, refunds and plan changes |
| `connect/` | Debezium connector config and registration |
| `spark/jobs/` | `bronze_cdc`, `silver_cdc`, `gold_star`, `dq_checks`, `maintenance`, `export_bronze`, `load_bronze`, plus `lakehouse_env` (local vs Glue differences) |
| `airflow/dags/` | local DAGs (gold every 10 min, nightly maintenance) and AWS DAGs |
| `infra/` | Terraform: S3, Glue catalog and jobs, IAM, Athena, budget, logs |
| `tools/` | reconciliation, Glue/Athena helpers, key management, teardown |
| `tests/` | PySpark unit tests |
| `docs/` | one write-up per build step: design decisions, measurements, gotchas |

## Run it locally

Needs Docker (about 7 GB of memory for the full stack) and Python 3.12.

```bash
cp .env.example .env                      # fill in passwords and keys
docker compose up -d --wait               # Postgres, Kafka, Debezium, Spark, Airflow, ...
python -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m generator seed        # 30 days of history (~660K payments)
./connect/register.sh                     # start CDC
.venv/bin/python -m generator run         # live traffic (Ctrl+C to stop)
./spark/sql.sh -e "SELECT status, count(*) FROM silver.payments GROUP BY status"
```

Airflow: http://localhost:8088 · Kafka UI: http://localhost:8080 · Spark UIs: :4040 (bronze), :4041 (silver).

## Run the batch layer on AWS

```bash
aws login --region ap-southeast-2
cd infra && cp terraform.tfvars.example terraform.tfvars    # set alert_email
terraform init && terraform apply && cd ..
./tools/put_pii_key.sh                                       # hash key -> SSM SecureString
./tools/create_airflow_key.sh && docker compose up -d airflow
docker compose exec airflow airflow dags trigger lakehouse_aws
./tools/teardown_aws.sh                                      # delete everything afterwards
```

A full AWS run (ship a batch, then load, silver, gold and checks on Glue, then an
Athena report) takes about 10 minutes and a few US cents. All the Glue runs in
this project together used about 1.5 DPU-hours, well under one US dollar.

## Documentation

1. [Postgres source](docs/01-postgres-source.md) · 2. [Data generator](docs/02-data-generator.md) ·
3. [Kafka and Debezium CDC](docs/03-kafka-debezium.md) · 4. [Spark streaming into bronze](docs/04-spark-bronze.md) ·
5. [Silver](docs/05-silver.md) · 6. [Gold star schema and SCD2](docs/06-gold.md) ·
7. [Data quality and Airflow](docs/07-quality-and-orchestration.md) · 8. [AWS foundations](docs/08-aws-foundations.md) ·
9. [Landing batches to AWS](docs/09-landing-and-bronze-on-aws.md) · 10. [Batch layer on Glue](docs/10-batch-layer-on-glue.md) ·
11. [Airflow drives AWS](docs/11-airflow-drives-aws.md) · 12. [Tests, CI and teardown](docs/12-tests-ci-teardown.md)

## Limits

- The data is synthetic, shaped to behave like real payment traffic.
- Ingestion runs on one machine: a single Kafka broker, Airflow in standalone mode.
- On AWS only the batch layer runs; data arrives as batches exported from the local bronze.
- Airflow reaches AWS with a narrowly scoped IAM user key; inside AWS it would use an execution role instead.
