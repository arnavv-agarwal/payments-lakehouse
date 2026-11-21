# Phase 1, Step 7: Data-quality checks and Airflow orchestration

## What this step builds

The streaming jobs (bronze, silver) already run on their own. This step adds
everything around them: scheduled gold builds, automated checks that alert,
and table housekeeping, all run by **Apache Airflow**.

```
Airflow (standalone, LocalExecutor) ──► docker-proxy ──► short-lived Spark containers
 lakehouse_gold         every 10 min:    build_gold ─┬─► check_incremental_matches_full_rebuild
                                                     └─► data_quality_checks ──► ops.dq_results
 lakehouse_maintenance  02:30 IST daily: maintain_bronze ─► maintain_silver ─► maintain_gold
```

| File / service | Purpose |
|---|---|
| `spark/jobs/dq_checks.py` | 35 checks across silver and gold; results appended to `lakehouse.ops.dq_results` |
| `spark/jobs/maintenance.py` | Compaction, delete-file cleanup, snapshot expiry, orphan-file removal for one layer |
| `airflow/dags/lakehouse.py` | The two DAGs; every task is a `DockerOperator` running one Spark job |
| `airflow` service | Airflow 3.3.2 `standalone`, UI on **http://localhost:8088** (no login, local only) |
| `airflow-db` service | Airflow's metadata database |
| `docker-proxy` service | Lets Airflow start containers without holding the raw Docker socket |

## Measured results

| Test | Result |
|---|---|
| 35 checks on a live system | all pass; silver measured ~45–65 s behind Postgres under load |
| **Outage drill**: silver job stopped for ~9 min while traffic continued | DAG run **failed**: 4 freshness errors, lag measured at 526–541 s (true value ≈ 530 s) |
| Recovery: silver restarted | it applied the backlog in one 1,834-row batch; next DAG run green |
| Gold consistency check inside the DAG, during live traffic | PASS every run (recomputes from the exact snapshots `build_gold` read) |
| Maintenance via Airflow | bronze payments 45 → 1 data files, silver payments delete files 12 → 0; ~10 s per layer |
| Final strict reconciliation (traffic stopped) | **PASS**: 737,259 rows, every column |
| Docker proxy | container/image calls allowed; a volumes API call refused with HTTP 403 |

## Key concepts

**What the checks cover.**

| Dimension | Checks |
|---|---|
| Accuracy | settled silver rows match Postgres (`*_settled_rows_match_source`) |
| Completeness | no source rows missing in silver; no deletes that never arrived |
| Freshness | silver ≤ 5 min behind the source; gold built within 30 min |
| Uniqueness | primary keys in silver, surrogate keys in every dimension, `payment_id` in the fact |
| Referential integrity | every fact row has its merchant version, customer, payment method, date |
| Validity | amounts positive, fee and refunds never above the amount, captured rows have `captured_at` |
| Model integrity | SCD2: one current version per merchant, no gaps or overlaps |
| Privacy | `silver.customers` has no name, email or phone column |
| Plausibility | today's success rate between 70% and 99% (warn) |

**Severity.** `error` fails the task, which fails the DAG run: someone has to
act. `warn` is recorded only: worth a look, and it often resolves itself (a
delete still in flight, an inferred member about to be resolved).

**Checking a live system: compare settled data only.** While data streams,
recent source changes are legitimately not in silver yet. The checks compare
rows whose last change is more than 10 minutes old, so they're usable around
the clock. The strict `reconcile_silver.py` still exists for quiet moments.

**Measuring freshness correctly (found in this step).** The first version
measured `max(source updated_at) − max(silver updated_at)`. After 17 idle
minutes, traffic resumed and that difference jumped to 1,011 s, a false alarm,
although silver was only ~60 s behind. The right measure is **how long the
oldest unapplied change has been waiting** (0 if none). That number is what the
outage drill measured accurately.

**Results are data.** Every run appends its 35 results to `ops.dq_results`.
You can chart failures over time, see when a check first failed, and alert on
trends rather than single runs.

**Airflow orchestrates, it doesn't compute.** Tasks start containers from the
Spark image and wait for them. The Airflow container stays small, a Spark
job can't starve the scheduler, and every job runs exactly as it would by hand.
In Phase 2 the same DAG shape submits jobs to EMR Serverless instead.

**Retries are only safe because jobs are idempotent.** Each task has 1 retry.
A retried gold build recomputes from its last saved watermark, and the MERGE
gives the same result, so a retry can never double-count.

**DAG settings that matter.**
- `max_active_runs=1`: gold runs never overlap (its incremental state assumes one writer).
- `catchup=False`: after downtime, don't queue up every missed 10-minute interval.
- Timezone-aware schedule: `30 2 * * *` in `Asia/Kolkata`, which is 21:00 UTC.

**Why table maintenance is necessary.**
- **Compaction** (`rewrite_data_files`): streaming creates many small files.
- **Delete files** (silver is merge-on-read): applying them on every read gets slower until they're rewritten away.
- **Snapshot expiry**: every commit keeps old files alive for time travel. But silver reads bronze snapshot by snapshot, so expiring snapshots silver hasn't read yet would break the stream. A 24-hour retention is the safety margin.
- **Orphan files**: written by failed commits, referenced by nothing. Only deleted when older than 3 days, so an in-progress write is never touched.

## Design choices

| Choice | Reason |
|---|---|
| `DockerOperator` + one image for all jobs | Same code path manually, in Airflow and (Phase 2) on EMR |
| Docker socket proxy (containers + images only) | Least privilege. Anything that can start containers is still powerful; the proxy narrows it rather than making it safe |
| Secrets in `private_environment` | Unlike `environment`, not shown in the UI's rendered templates or logs |
| `airflow standalone`, all users admin | One container for a laptop; production splits the components and uses real auth (Phase 2: MWAA + IAM) |
| `--check` pinned to the snapshots of the run it verifies | Like-for-like comparison even while silver keeps changing |
| Maintenance one layer at a time at 02:30 IST | Lowest traffic; fewer concurrent commits with the streaming jobs |
| `partial-progress.enabled` for compaction | If a streaming commit conflicts, only one file group retries, not the whole rewrite |

## Gotchas found in this step

1. **The freshness false alarm** described above.
2. **Iceberg procedure arguments must be literals**: `older_than => current_timestamp() - INTERVAL 24 HOURS` fails to bind; compute the cutoff and pass `TIMESTAMP '...'`.
3. **`remove_orphan_files` lists files through Hadoop by default** (`No FileSystem for scheme "s3"`). With S3FileIO, pass `prefix_listing => true`.
4. **A PySpark `Row` is a tuple**: `"col" in row` tests values. Use `"col" in row.asDict()`.
5. **`DockerOperator` mounts a temp dir from the Airflow container by default**, a path the Docker host doesn't have; set `mount_tmp_dir=False`.
6. **Bind-mount sources are paths on the Docker host**, not in the Airflow container. The project path is passed in as `LAKEHOUSE_HOST_DIR` (from `${PWD}`), so run `docker compose` from the project folder.
7. **Port clash**: Airflow and Kafka UI both default to 8080; Airflow is on 8088.
8. **A few delete files survive compaction** in silver partitions that hold a single data file (Iceberg skipped those groups under the current options). Harmless at this size; worth revisiting at scale.

## Interview questions this step prepares you for

1. What data-quality checks do you run, and how do you decide error vs warning?
2. How do you check accuracy on a system that never stops writing?
3. How do you measure data freshness? What's wrong with comparing max timestamps?
4. What should Airflow do, and what shouldn't it? Why not run Spark on the Airflow worker?
5. What makes a task safe to retry?
6. `catchup`, `max_active_runs`, retries, timezones: which matter for a pipeline like this, and why?
7. What maintenance does an Iceberg (or Delta) table need, and what breaks without it?
8. How can snapshot expiry break a streaming reader?
9. Walk through an incident: a streaming job dies at 2 am. How is it detected, and what happens to the data? (The outage drill.)

## Commands

```bash
open http://localhost:8088                                    # Airflow UI
docker compose exec airflow airflow dags list
docker compose exec airflow airflow dags trigger lakehouse_gold
docker compose exec airflow airflow dags pause lakehouse_gold # stop the 10-minute runs

# Run the checks or maintenance by hand
docker compose run --rm spark-tools /opt/spark/bin/spark-submit \
  --py-files /opt/lakehouse/jobs/silver_cdc.py /opt/lakehouse/jobs/dq_checks.py
docker compose run --rm spark-tools /opt/spark/bin/spark-submit /opt/lakehouse/jobs/maintenance.py --layer silver

./spark/sql.sh -e "SELECT ran_at, check_name, violations, details FROM ops.dq_results WHERE NOT passed ORDER BY ran_at DESC"
```
