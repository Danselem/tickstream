# Cost

Guardrails (Brief §10) and a rough model. Actuals replace estimates as phases land.

## Guardrails

1. `aws_budgets_budget` (forecast 80 %, actual 100 % of $20/mo) is the first resource
   created — alert lands before any spend is possible.
2. Least-privilege IAM user; no costly resources (Redshift, big instances) — out of scope.
3. Partition by day+symbol; every Athena query and GX scan is partition/window-pruned.
4. Micro-batch commits (30–60 s), not per-event: fewer S3 requests, fewer small files.
5. `OPTIMIZE` + `VACUUM` on a schedule (Phase 2) keep file counts and storage down.
6. GX validates locally (ADR-002): zero Athena spend for quality gates.
7. Tear down with a single `tofu destroy` (approval required).

## Rough model (XRP only, ~200 trades/s)

- ~17M events/day → bronze Parquet ≈ 1–2 GB/day → 30-day retention ≈ tens of GB on S3
  (pennies/month).
- Athena: dbt builds every ~10 min scan only new partitions; dashboards query gold
  aggregates. Expected single-digit dollars/month; budget alert at $20 is the backstop.

## What I'd change at scale

Tracked here in Phase 4: S3 Tables auto-compaction, Athena engine/capacity pricing,
lifecycle-tiering bronze to Infrequent Access after N days.
