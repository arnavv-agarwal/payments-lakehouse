# Phase 2, Step 11: Airflow drives the AWS pipeline

## What this step builds

The local Airflow now runs the cloud side end to end. Two new DAGs in
`airflow/dags/lakehouse.py`, both **manual** (`schedule=None`: each run costs
Glue time):

```
lakehouse_aws
  choose_batch_id ─► export_new_bronze_rows ─► new_rows_to_ship ─► upload_batch_to_s3
   (Python)           (DockerOperator, local    (ShortCircuit:      (S3Hook, manifest last)
                       Spark: export_bronze.py)  nothing new → skip
                                                 everything after)
  ─► load_bronze ─► build_silver ─► build_gold ─┬─► check_incremental_matches_full_rebuild ─┐
     (GlueJobOperator for each)                  └─► data_quality_checks ─────────────────────┴─► report_from_gold_with_athena
                                                                                                  (AthenaHook)

lakehouse_aws_maintenance
  maintain_bronze ─► maintain_silver ─► maintain_gold          (GlueJobOperator, --layer)
```

| Piece | What it is |
|---|---|
| `infra/airflow_access.tf` | IAM user `payments-lakehouse-airflow` and its policy |
| `tools/create_airflow_key.sh` | creates/rotates the user's access key straight into `.env`; prints only the last 4 characters |
| `docker-compose.yml` (airflow) | `AIRFLOW_CONN_AWS_LAKEHOUSE` (connection as JSON, built from `.env`), `AWS_LAKEHOUSE_BUCKET`, `./landing` mounted read-only |
| `infra/glue_jobs.tf` | new job `payments-lakehouse-gold-check` (gold_star.py with `--check true`) |

## Decisions

**How a laptop process authenticates to AWS.** Airflow runs outside AWS, so
it can't take on an IAM role the way a service inside AWS does. Options:
- the developer's own `aws login` session: full account access, and it
  expires within hours, so it can't run unattended. Rejected.
- **a dedicated IAM user with a narrowly scoped policy and an access key**:
  chosen. The key is written by a script into `.env` (gitignored), never into
  Terraform state; re-running the script rotates it (old keys deleted first,
  so only one is ever valid). `terraform destroy` removes the user and its
  key (`force_destroy`).
- In production on AWS (MWAA, or Airflow on ECS/EKS), the execution role
  would replace the key entirely; from outside AWS, IAM Roles Anywhere
  (certificate-based temporary credentials) avoids long-lived keys.

**What the Airflow user may do** (and nothing else):

| Allowed | Scope |
|---|---|
| `s3:PutObject` | `landing/*` only |
| start / get / stop Glue job runs, `glue:GetJob` | jobs named `payments-lakehouse-*` |
| read Glue job logs | `/aws-glue/jobs/*` (for `verbose=True`) |
| run Athena queries | the `payments-lakehouse` workgroup |
| read the catalog and files | **gold only** (catalog entries and `warehouse/gold/*`) |
| write query results | `athena-results/*` |

It can't read bronze or silver (raw and personal data), change tables,
touch other jobs or manage IAM. Starting a Glue job doesn't need
`iam:PassRole`: the role is fixed in the job definition.

**One batch id per run.** `choose_batch_id` generates it (UTC timestamp) and
every later task takes it from XCom. Not `{{ ts_nodash }}`: in Airflow 3 a
manually triggered run has no logical date (`logical_date=None`).

**Nothing new, nothing spent.** If the export finds no new rows it writes no
manifest; the ShortCircuitOperator then skips every AWS task.

**The manifest is uploaded last,** after all data files, so the load job can
never see a manifest whose files aren't all in S3 yet.

**GlueJobOperator settings.** `verbose=True` copies the Glue job's output
into the Airflow task log; `stop_job_run_on_kill=True` stops the billed Glue
run if the task is cancelled; `resume_glue_job_on_retry=True` makes a retry
wait for a still-running run instead of starting a second one.

**A separate Glue job for the gold check.** The first full run's check task
failed once with `ConcurrentRunsExceededException` and passed on Airflow's
retry: it started `payments-lakehouse-gold-star` seconds after the build had
finished on the same job (one run at a time, and Glue keeps a finished run's
slot busy briefly). The check now has its own job,
`payments-lakehouse-gold-check` (same script, `--check true` by default).
Concurrency limits are per job, so the build is still single-writer.

## Measured results

| Run | Result |
|---|---|
| Airflow's AWS identity | `user/payments-lakehouse-airflow`, region `ap-southeast-2` |
| **Run 1: nothing new to ship** | export found 0 rows → `new_rows_to_ship` returned False → all 7 AWS tasks **skipped**; no Glue run |
| **Run 2: after 2 min of traffic** | all 10 tasks succeeded in 9 min 10 s; batch 20261122T081933Z (1,269 rows: 1,196 payment events); silver `batch 2` (+423 payment rows); gold: 424 payments recomputed, 13 new merchant versions; check PASS (after one retry, see above); 25 DQ checks pass |
| Run 2 Athena report | success rate UPI 91.0%, card 87.1%, wallet 95.0%, netbanking 84.2%; fees by plan in force at payment time: standard ₹1.77 crore, growth ₹62.9 lakh, enterprise ₹5.37 lakh |
| **`lakehouse_aws_maintenance`** | 3 Glue runs (bronze → silver → gold), all succeeded; e.g. `silver.merchants` data files 6 → 1, delete files 3 → 1; `silver.payments` 35 → 32 data files. No snapshots expired (all younger than the 24 h retention), no orphan files |
| **Run 3: after the gold-check job split, 1 min of traffic** | all 10 tasks succeeded **on the first attempt** in 9 min 56 s; gold: 205 payments recomputed, 10 new merchant versions; check PASS; 25 DQ checks pass; fees now printed as decimals (standard ₹1,76,75,471.64) |

## Gotchas

1. **A triggered run of a paused DAG just waits** (`queued`) until the DAG is
   unpaused. New DAGs start paused. Unpausing a `schedule=None` DAG doesn't
   start anything by itself.
2. **Airflow 3 manual runs have `logical_date=None`**, so templates based on
   it (`ds`, `ts_nodash`) can't name a batch.
3. **Athena returns `sum(x) / 100.0` as a DOUBLE** and prints `1.767146528E7`.
   Money is cast to `DECIMAL(18, 2)`.
4. **Two jobs' runs back to back on one Glue job** hit the concurrency limit
   (see above). Separate jobs, or a retry, avoid it.

## Commands

```bash
./tools/create_airflow_key.sh && docker compose up -d airflow   # once, and to rotate the key
docker compose exec airflow airflow dags unpause lakehouse_aws  # once (it has no schedule)
docker compose exec airflow airflow dags trigger lakehouse_aws  # one run
docker compose exec airflow airflow dags trigger lakehouse_aws_maintenance
```

In the UI (http://localhost:8088): `lakehouse_aws` → a run → a task → Logs.
The Glue tasks' logs include a link to the run in the AWS console and the
job's own output.

## Interview questions this step prepares you for

- How does an on-premise scheduler get AWS credentials safely? What would you do differently inside AWS?
- What does your orchestrator do when there's no new data?
- How do you avoid paying for a cloud job that the orchestrator has given up on?
- How do you stop two runs of a job writing at the same time, and what went wrong the first time?
