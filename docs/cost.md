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

After the 7-coin expansion (18 Oct): the lake is **≈1.08 GB / 14,505 objects**
(up from 481 MB / 11k with XRP alone). Storage is still ≪ $0.10/mo; the object
count — and thus request cost — is what OPTIMIZE + lifecycle rules target. The
whole-table integrity check (ADR-016) scans locally, so it adds **$0** Athena
spend (~19 s for 4.3M key rows).

Model check: 7 coins run ~150–200 events/s (~15M/day). Bronze Parquet ≈ tens of
bytes/row → ~1 GB/day; 30-day retention ≈ tens of GB ≈ single-digit dollars
all-in. Athena stays in cents while every query prunes to day+symbol partitions.

## Actuals (16–24 Oct 2026)

| Service | Spend | Notes |
|---|---|---|
| S3 | $3.28 | 8.9 GB / 59,739 objects before maintenance; 6.9 GB / 55,760 after OPTIMIZE + VACUUM. Requests, not storage, dominate. |
| Athena | $0.12 | dbt builds + maintenance; GX and integrity checks local. |
| Glue | $0.00 | Catalog updates only. |

The jump from the first fortnight reflects higher live volume plus many small
Iceberg files accumulated while the scheduler was stopped (host sleep). One
maintenance pass cut file count and bytes; running it more often (or lifecycle
rules) keeps request cost down. The kline backfill added 846,720 rows in a single
table rewrite; the ticker stream adds ~5–7 msg/s and is negligible.

**Measurement.** AWS Cost Explorer, service-level, 16–24 Oct 2026 (9 days),
excluding the unrelated RDS line in the same account. S3 includes both the lake
and `athena-results/`. The window included recovery/backfill, so it is not a
steady-state month.

**Run-rate.** $3.40 / 9 days ≈ $0.38/day ≈ **$11–12/month** at this volume;
steady state without backfill and with regular compaction should be lower. The
$20 budget alert leaves headroom. Storage is still cents — the bill is dominated
by PUT/GET requests from many small objects.

## Recommendations (not yet applied)

- Lifecycle rule on `s3://<bucket>/athena-results/` (expire after 30 days):
  dbt tests + SQL Lab leave result files behind — a large share of the current
  55.8k objects.
- Lifecycle bronze prefixes to Intelligent-Tiering after 90 days.
- Run maintenance on a predictable weekly cadence (not only after incidents) so
  object counts do not spike; measure Cost Explorer before/after.
- If dashboards go multi-user: Athena provisioned capacity or result reuse;
  Superset query caching + Postgres metadata.

## What I'd change at scale

S3 Tables auto-compaction instead of scheduled OPTIMIZE; Athena
capacity pricing when concurrent; lifecycle-tiering bronze to Infrequent Access
after N days; per-workgroup cost allocation tags.
