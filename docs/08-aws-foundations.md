# Phase 2, Step 8: AWS foundations with Terraform

## What this step builds

Phase 2 runs the batch side of the lakehouse on AWS for a few runs. This step
creates the cloud resources everything else needs, all written as Terraform
code in `infra/`, so one `terraform destroy` removes them at the end.

```
infra/ (Terraform, local state) ──► AWS account, ap-southeast-2 (Sydney)
   budget.tf    AWS Budgets: $5/month, email at 20% / 100% actual and 100% forecast
   storage.tf   S3 bucket payments-lakehouse-<account-id>
                  warehouse/ landing/ code/ tmp/ athena-results/
   catalog.tf   Glue Data Catalog databases: bronze, silver, gold, ops
   iam.tf       IAM role payments-lakehouse-glue-job (what Glue jobs run as)
   athena.tf    Athena workgroup payments-lakehouse (1 GiB scan limit per query)
```

| Phase 1 (local) | Phase 2 (AWS) |
|---|---|
| RustFS (S3 API) | Amazon S3 |
| Iceberg JDBC catalog on Postgres | AWS Glue Data Catalog |
| Spark containers started by Airflow | AWS Glue 6.0 jobs (Spark 4.1.1, Iceberg 1.11.0) |
| `spark/sql.sh` | Amazon Athena (engine v3) |
| `.env` secrets | IAM role for the jobs; secrets added in a later step |

## Decisions

**AWS Glue jobs instead of EMR Serverless.** The account was created through
AWS's new sign-up flow, which starts on a Free plan with $100 of credits.
Amazon EMR is only available on the Paid plan, and the upgrade can't be
reversed. AWS Glue is in the Free plan, and Glue 6.0 (August 2026) runs Spark
4.1.1 with Iceberg 1.11.0, close to the local Spark 4.1.4 / Iceberg 1.12.0.
Staying on the Free plan also means the account can't be billed beyond its
credits. Glue is also the more common choice in Indian data-engineering job
descriptions.

**Region: Sydney.** New-style accounts are pinned to one region chosen by the
contact country; India maps to `ap-southeast-2`. A service control policy (SCP)
from AWS's organization enforces it:

```
glue:GetDatabases in ap-south-1 → AccessDeniedException ... with an explicit deny in a service control policy
glue:GetDatabases in us-east-1  → same
glue:GetDatabases in ap-southeast-2 → OK
```

Latency to Sydney doesn't matter for batch runs.

**Short-lived credentials, no access keys.** The CLI signs in with
`aws login` (AWS CLI ≥ 2.32): a browser sign-in that issues temporary
credentials for the role `AccountFullAccessRole`, refreshed automatically.
No long-lived access key is stored on disk. The Terraform AWS provider
(6.73.0) uses the same credentials.

**Terraform with local state.** `terraform.tfstate` stays in `infra/`
(gitignored). One person, a few days of use: a remote S3 backend with locking
would be the team setup. The provider lock file `.terraform.lock.hcl` is
committed so every machine uses the same provider build.

**Budget measured before credits.** `include_credit = false`. With credits
included, the measured cost stays at $0 until the credits run out, so the
alert would never fire while it matters. Budgets only send emails; they don't
stop resources. They refresh a few times a day, so an alert can lag usage by hours.

**One bucket, locked down.**
- Public access blocked at the bucket level; object ACLs disabled (`BucketOwnerEnforced`).
- Default encryption SSE-S3 (AES-256).
- Bucket policy denies any request without TLS (`aws:SecureTransport = false`).
- No versioning: Iceberg snapshots already keep table history, and versioning
  would keep every file that Iceberg maintenance deletes.
- Lifecycle: `athena-results/` and `tmp/` expire after 7 days; incomplete
  multipart uploads are aborted after 1 day.
- `force_destroy = true` so teardown deletes the data too. Right for synthetic
  demo data; never for real data.

**Same namespace names as Phase 1.** Each Glue database is an Iceberg
namespace (`bronze`, `silver`, `gold`, `ops`), so the jobs' SQL is unchanged;
only the catalog configuration differs.

**Least-privilege job role.** The Glue job role can:
- list, read, write and delete objects in the lakehouse bucket only;
- get, create, update and delete tables in the four databases only (Iceberg
  commits are `glue:UpdateTable` calls with optimistic locking);
- write logs under `/aws-glue/*` and metrics in the `Glue` namespace.

It can't delete the bucket or the databases, touch other buckets or
databases, or use IAM.

**Athena guard rail.** The workgroup enforces its own settings (users can't
override the result location or limits) and cancels any query that would scan
more than 1 GiB (about $0.005 at $5/TB).

## Verification

| Check | Result |
|---|---|
| `terraform apply` | 14 resources created |
| Budget | $5.00 monthly, `IncludeCredit = false`, notifications ACTUAL 20%, ACTUAL 100%, FORECASTED 100% |
| Anonymous HTTPS request to the bucket | 403 |
| Request with valid credentials over plain HTTP | `AccessDenied ... explicit deny in a resource-based policy` (the TLS-only policy) |
| Write, read and delete an object over HTTPS | works; object encrypted `AES256` |
| Athena `SELECT 'athena works', current_timestamp` in the workgroup | SUCCEEDED on engine v3; result file written to `athena-results/`; cutoff 1073741824 bytes, enforced |
| Glue databases | `bronze gold ops silver` |
| Second `terraform plan` | no changes (exit code 0): the AWS side matches the code |

IAM policy simulator, job role (with `aws:RequestedRegion = ap-southeast-2`):

| Action | Decision |
|---|---|
| `s3:PutObject` / `s3:GetObject` on the lakehouse bucket | allowed |
| `s3:GetObject` on any other bucket | implicitDeny |
| `s3:DeleteBucket` on the lakehouse bucket | implicitDeny |
| `glue:UpdateTable` on `silver.payments`, `glue:CreateTable` in `gold` | allowed |
| `glue:UpdateTable` on a table in `default` | implicitDeny |
| `glue:DeleteDatabase` on `silver` | implicitDeny |
| `logs:PutLogEvents` under `/aws-glue/` | allowed |

## Gotchas

1. **The policy simulator reported `explicitDeny` for everything at first**,
   even for actions the role clearly allows. The account's region SCP denies
   requests whose `aws:RequestedRegion` isn't Sydney, and the simulator treats a
   missing context key as "not Sydney". Passing
   `--context-entries ContextKeyName=aws:RequestedRegion,ContextKeyValues=ap-southeast-2,ContextKeyType=string`
   gives the real answer. The detail field `OrganizationsDecisionDetail`
   shows when an SCP is the cause.
2. **zsh modifiers in shell variables.** `"...:$ACCT:table/..."` turned into
   `...<account-id>able/...` (the `:t` vanished) because zsh reads `$ACCT:t`
   as "the tail of `$ACCT`". Use `${ACCT}` inside strings with colons.
3. **The S3 lifecycle configuration took about 1 minute to create.** The
   provider waits until S3 reports the rules consistently; that's normal.
4. **`account:GetPrimaryEmail` is denied** for the new-style account's role,
   so the budget email is a Terraform variable rather than looked up.

## Cost

All of these are free while idle: budgets (monitoring only), an empty bucket,
Glue catalog entries (first million objects free), an IAM role, and an Athena
workgroup. Costs start with data in S3 (cents per GB per month), Glue job runs
(per DPU-hour, 1-minute minimum) and Athena scans ($5 per TB).

## Commands

```bash
aws login --region ap-southeast-2        # browser sign-in, short-lived credentials
aws sts get-caller-identity              # which account and role am I?

cd infra
cp terraform.tfvars.example terraform.tfvars   # then set alert_email
terraform init                           # download the AWS provider (once)
terraform plan                           # show what would change
terraform apply                          # make it so
terraform output                         # bucket, role ARN, workgroup...
terraform destroy                        # delete everything (end of Phase 2)
```

## Interview questions this step prepares you for

- Why Terraform rather than clicking in the console? Where is the state, and what would you change for a team?
- What's the difference between an IAM user and a role? Why do jobs run as a role?
- How do you make sure a bucket can never become public?
- What is an SCP, and how does it combine with IAM policies? (An SCP sets the
  maximum; IAM grants within it; an explicit deny anywhere wins.)
- Why might an AWS budget alert never fire on a credit-funded account?
- Glue vs EMR (on EC2) vs EMR Serverless: when would you pick each?
