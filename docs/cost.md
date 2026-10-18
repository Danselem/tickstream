# Cost

Guardrails (Brief §10). Estimates became actuals once the pipeline ran.

## Guardrails

1. `aws_budgets_budget` (forecast 80 %, actual 100 % of $20/mo) is the first resource
   created — alert lands before any spend is possible.
2. Least-privilege IAM user; no costly resources (Redshift, big instances) — out of scope.
3. Partition by day+symbol; every Athena query and GX scan is partition/window-pruned.
4. Micro-batch commits (45 s / 20k rows), not per-event: fewer S3 requests, fewer small files.
5. `OPTIMIZE` + `VACUUM` on a schedule keep file counts and storage down.
6. GX validates locally (ADR-002): zero Athena spend for quality gates.
7. Tear down with a single `tofu destroy` (approval required).

## Actuals (1–15 Oct 2026, single account)

| Service | Spend | Notes |
|---|---|---|
| S3 | $0.61 | 481 MB, ~11k objects (lake + `athena-results/`). Storage itself is pennies; requests dominate. |
| Athena | **$0.009** | Partition-pruned dbt builds + dashboard queries; GX runs locally. |
| Budget alert | $0 used of $20 | Never close. (RDS $0.12 in the same account is unrelated to this project.) |

Model check: 7 coins run ~150–200 events/s (~15M/day). Bronze Parquet ≈ tens of
bytes/row → ~1 GB/day; 30-day retention ≈ tens of GB ≈ single-digit dollars
all-in. Athena stays in cents while every query prunes to day+symbol partitions.

## Recommendations (not yet applied)

- Lifecycle rule on `s3://<bucket>/athena-results/` (expire after 30 days):
  dbt tests + SQL Lab leave result files behind — most of the 11k objects.
- Lifecycle bronze prefixes to Intelligent-Tiering after 90 days.
- If dashboards go multi-user: Athena provisioned capacity or result reuse;
  Superset query caching + Postgres metadata.

## What I'd change at scale

S3 Tables auto-compaction instead of scheduled OPTIMIZE; Athena
capacity pricing when concurrent; lifecycle-tiering bronze to Infrequent Access
after N days; per-workgroup cost allocation tags.
