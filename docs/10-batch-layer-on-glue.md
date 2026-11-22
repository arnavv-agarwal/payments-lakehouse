# Phase 2, Step 10: Silver, gold and data-quality checks on AWS Glue

## What this step builds

The rest of the batch layer now runs on AWS Glue, with the **same job files**
as locally:

```
landing batch ─► load_bronze ─► silver_cdc ─► gold_star ─┬─► gold_star --check true
   (step 9)       (Glue)        (Glue,         (Glue)    └─► dq_checks            (Glue)
                                 available-now,
                                 checkpoint in S3)
```

| Glue job | Script | Default arguments |
|---|---|---|
| `payments-lakehouse-load-bronze` | `load_bronze.py` | `--landing s3://<bucket>/landing` |
| `payments-lakehouse-silver-cdc` | `silver_cdc.py` | `--available-now true --checkpoint-root s3://<bucket>/checkpoints/silver --pii-key-param /payments-lakehouse/pii-hash-key` |
| `payments-lakehouse-gold-star` | `gold_star.py` | (incremental); `--check true` per run to compare with a full rebuild |
| `payments-lakehouse-dq-checks` | `dq_checks.py` | `--skip-source-checks true --settle-minutes 0` |
| `payments-lakehouse-maintenance` | `maintenance.py` | `--layer` per run |

All: Glue 6.0, 2 × G.1X, 15-minute timeout, no Glue retries, one run at a time.
Every job gets `--extra-py-files` for the modules it imports
(`lakehouse_env.py`; `dq_checks` also `silver_cdc.py`).

## What had to change in the jobs

All environment differences live in one new module, `spark/jobs/lakehouse_env.py`:

| Difference | Local | Glue | Handled by |
|---|---|---|---|
| Catalog access | JDBC catalog + password from `.env` | Glue Data Catalog via the job's IAM role | `spark_session()`: sets the JDBC password only if the env var exists |
| Boolean options | `--check` | Glue passes every argument with a value: `--check true` | `add_flag()`: `nargs="?"`, `const=True`, parses `true/false` |
| Extra arguments | none | Glue adds `--JOB_ID`, `--JOB_RUN_ID`, `--TempDir`, ... | `parse_args()`: ignores unknown arguments on Glue only; locally a typo is still an error |
| Secrets | `PII_HASH_KEY` env var | SSM Parameter Store SecureString | `secret()`: env var first, else `ssm:GetParameter` with decryption |
| Exit status | `sys.exit(0)` = success | **any `SystemExit`, even 0, = FAILED run** | `finish(failed)`: exit 1 on failure, otherwise return normally |

Job-specific changes:
- `silver_cdc.py`: `--checkpoint-root` (default unchanged: `/checkpoints/silver`
  or `$CHECKPOINT_ROOT`), `--pii-key-param`; `pii_hasher(key)` takes the key.
  On Glue it runs as a batch with `availableNow`: process all new bronze
  snapshots, commit, stop. The streaming checkpoint in S3 remembers where it
  stopped, so the next run continues from there (`batch 1`, `batch 2`, ...).
- `dq_checks.py`: `--skip-source-checks` (no Postgres on AWS) and
  `--settle-minutes`. New check per table, used in both places:
  **`silver_<table>_matches_bronze`**: for every key, the highest WAL position
  (`source.lsn`) in bronze must equal silver's `_lsn`. Locally that makes
  40 checks (35 + 5); on AWS 25 (the 15 Postgres-based checks are skipped).
- `gold_star.py`, `maintenance.py`, `load_bronze.py`: only the shared session,
  argument and exit handling.

**The hash key.** `tools/put_pii_key.sh` copies `PII_HASH_KEY` from `.env`
into Parameter Store (`/payments-lakehouse/pii-hash-key`, SecureString,
AWS-managed key `aws/ssm`) through a mode-600 temporary file, so the value
never appears in a command line, a log or Terraform state. The job role may
read that one parameter, and decrypt only via Parameter Store
(`kms:ViaService = ssm.<region>.amazonaws.com`). Same key as locally, so
`email_hash` values are identical in both environments.

**A Terraform rename.** The step-9 script upload `aws_s3_object.job_script`
became `aws_s3_object.job_code` (it now uploads helper modules too). A
`moved` block tells Terraform it's the same object; without it Terraform
would delete and recreate the object at the same key, and the delete could
remove the new upload.

## Measured results

| Run | Result |
|---|---|
| Local regression after the code changes | silver stream restarted fine; gold incremental + `--check` PASS; 40 DQ checks pass; an unknown argument is still rejected; maintenance runs |
| **silver on Glue (first run)** | 102 s; created and filled all 5 tables (664,865 payments) |
| AWS silver vs local silver | identical per table: rows, deletes, sum of `_lsn`, amounts, distinct email hashes; the same customer has the same `email_hash` |
| **gold on Glue (first run = full refresh)** | 118 s; 681 merchant versions (407 current, 274 past), 664,865 payment facts, 0 inferred |
| gold `--check` and DQ on Glue, in parallel | both passed, but Glue marked both **FAILED** (`SystemExit: 0`); fixed with `finish()`; re-run: both SUCCEEDED |
| **Negative test**: Athena `UPDATE silver.merchants SET _lsn = _lsn - 1 WHERE merchant_id = 230` | DQ on Glue FAILED: `silver_merchants_matches_bronze 1 ... 1 at an older version`; restored with the inverse UPDATE; Athena check: 0 missing, 0 stale |
| **Incremental round** (2 min of traffic → batch 3, 1,173 rows) | load 129 s → silver 59 s (`batch 1`: +388 payment rows, merges with position deletes) → gold 79 s (388 payments recomputed, 16 new merchant versions) → check PASS + 25 DQ checks pass (in parallel) |
| **AWS gold vs local gold** after the round | identical: dimension counts, captured count, amount/fee/net/refund sums, distinct surrogate keys, attempt numbers, enterprise fees ₹5,36,081.30 |

Glue usage for steps 9 and 10 together: about 2,860 DPU-seconds (0.8 DPU-hours),
roughly 25–35 US cents depending on the regional Glue 6.0 rate. Athena scanned a
few MB per query.

## Gotchas

1. **Glue 6.0 fails a run on `sys.exit(0)`.** The error is literally
   `SystemExit: 0` with state FAILED. Return normally on success; exit
   non-zero only for real failures.
2. **Glue arguments always have values.** `--check` alone can't be passed;
   `--check true` can. The options accept both forms now, so the Airflow DAG's
   local `--check` still works.
3. **`parse_args()` breaks on Glue** because of the arguments Glue adds.
   Ignoring unknown arguments everywhere would hide typos locally, so it's
   conditional on running in Glue (`--JOB_RUN_ID` present).
4. **Executors and helper modules.** `silver_cdc.py` imports `lakehouse_env`
   inside `main()`, because other jobs import `silver_cdc`'s helpers and Spark
   may import it in Python workers, where `lakehouse_env` isn't needed.
5. **Athena can write to Iceberg.** `UPDATE` on a merge-on-read table writes
   a position-delete file and a new data file, as an ordinary Iceberg commit
   (`overwrite` in the snapshot list). Handy for tests; in production, writes
   should go through the pipeline.

## Commands

```bash
./tools/put_pii_key.sh                                   # once (and after changing the key)
./tools/glue_run.sh payments-lakehouse-load-bronze --batch-id <id>
./tools/glue_run.sh payments-lakehouse-silver-cdc
./tools/glue_run.sh payments-lakehouse-gold-star
./tools/glue_run.sh payments-lakehouse-gold-star --check true
./tools/glue_run.sh payments-lakehouse-dq-checks
./tools/glue_run.sh payments-lakehouse-maintenance --layer silver
./tools/athena_query.sh "SELECT check_name, violations FROM ops.dq_results WHERE NOT passed"
```

## Interview questions this step prepares you for

- How do you make one Spark codebase run both locally and on a managed service? What differs?
- How does a streaming job run as a scheduled batch? (`availableNow` + a durable checkpoint.)
- Where do secrets live on AWS, and how does a job read one without it appearing in logs or infrastructure state?
- How did you prove the cloud results are correct? (Independent engines, fingerprints, a negative test.)
- What's a position-delete file, and why did a one-row UPDATE create one?
