# Cost

**What this answers:** what the pipeline costs, why, and which guardrails are
actually active.

**Bottom line:** **$3.40** for 16–24 October 2026. That nine-day window included
recovery and a backfill, so it is not a steady-state month; the rough projection is
**$11–12/month** at current volume. S3 requests from many small files dominate. The
$20 budget alert leaves headroom.

## Why this bill looks the way it does

S3 charges for stored bytes **and** for requests. A small Parquet file is still an
object, and every write, read, list, and delete is a request. Frequent
micro-batches create many objects, so request count can cost more than storage.

Athena charges for data scanned, not for query time. Partitioning by day and
symbol, and filtering on those columns, keeps scans small. Great Expectations and
the integrity check scan locally through PyIceberg, so the quality gate adds no
Athena cost.

## Cost drivers

| Driver | What causes it | Current effect |
|---|---|---|
| S3 PUT/GET requests | Micro-batch writes, dbt reads, dashboard queries, many small files | **Dominant** |
| S3 storage | Iceberg data files, old snapshots, Athena results | Cents at this volume |
| Athena bytes scanned | dbt models and dashboard SQL | Cents because queries prune by day/symbol |
| Glue catalog | Table lookups and updates | Effectively zero |
| Data transfer | Local Docker to AWS | Small; no public egress |
| Kafka / compute | Runs locally in Docker | No cloud bill |

## Guardrails

| Guardrail | Status | Effect |
|---|---|---|
| $20/month AWS budget alert | Active | Forecast at 80%, actual at 100% |
| Budget alert declared first in the main stack | Active | The tiny state bucket and lock table are created during bootstrap; the alert is declared first in the main stack and is the first spend guardrail there. OpenTofu may create independent resources concurrently |
| Least-privilege IAM user | Active | Only the S3/Glue/Athena actions needed |
| No costly services | Active | No Redshift, large instances, or managed Kafka |
| Day + symbol partitions | Active | Athena reads a smaller slice |
| Batched writes (45 s / 20k rows) | Active | Fewer requests than per-event writes |
| Scheduled `OPTIMIZE` + `VACUUM` | Active | Controls file and snapshot count |
| Local GX and integrity scans | Active | $0 Athena spend for quality checks |
| S3 expiration/tiering rules | **Pending** | Athena results and old bronze still accumulate; only abort-incomplete-upload is configured |
| Teardown with approval | Manual | One `tofu destroy`, data is deleted |

## Measured spend

### 1–15 October 2026

| Service | Spend | Notes |
|---|---|---|
| S3 | $0.61 | 481 MB, ~11k objects (whole bucket, including `athena-results/`) |
| Athena | **$0.009** | Partition-pruned dbt builds and dashboard queries; GX local |
| Glue | $0.00 | Catalog only |

The account also had an unrelated RDS charge of $0.12; it is excluded here.

After the 7-coin expansion on 18 October, a **lake-only** check found **≈1.08 GB /
14,505 objects**. No lake-only baseline was recorded with XRP alone, so the growth
is directional only. Storing that much data is still less than $0.10/month; the
object count is the part that matters for requests.

That 18 October lake-only snapshot is not comparable to the 1–15 October
whole-bucket figure or the 16–24 October total, which include `athena-results/`
and old snapshots.

### 16–24 October 2026

| Service | Spend | Notes |
|---|---|---|
| S3 | $3.28 | 8.9 GB / 59,739 objects before maintenance; 6.9 GB / 55,760 after |
| Athena | $0.12 | dbt builds + maintenance; GX and integrity add no Athena spend |
| Glue | $0.00 | Catalog updates only |
| **Total** | **$3.40** | 9-day window, including recovery and backfill |

The jump from the first fortnight has several causes, not one:

- both periods use whole-bucket figures, but the later bucket held much more
  data and more Athena results and old snapshots;
- higher live volume with seven symbols;
- many small Iceberg files accumulated while the scheduler was stopped during a
  host sleep;
- the kline backfill, which added **846,720 rows** in one table rewrite;
- normal dbt and dashboard queries.

One maintenance pass cut both file count and bytes. This was **not** a
steady-state month: it included recovery and backfill.

**Measurement:** AWS Cost Explorer, service level, 16–24 October 2026, excluding
the unrelated RDS line. S3 includes the lake and `athena-results/`.

## Projection (not a measured steady state)

| Assumption | Value |
|---|---|
| Trade rate | ~150–200 events/s (~15M/day) |
| Quote rate | ~5–7 messages/s |
| Retention | 30 days of useful history (projection assumption, not measured) |
| Maintenance | Regular `OPTIMIZE` + `VACUUM` |
| Backfill | None in the projection |
| Athena queries | Always filtered by day and symbol |

At those assumptions, the rough run-rate is **~$11–12/month**, with S3 requests
still the largest line. Treat it as a planning estimate, not a bill. A month with a
large backfill, a long outage, or missed maintenance will be higher.

Storage growth is not the main risk. Object count is.

## Recommendations

Not yet applied:

1. Add a lifecycle rule to expire `s3://<bucket>/athena-results/` after 30 days.
   dbt tests and SQL Lab leave result files behind, and they are a large share of
   the object count.
2. Add lifecycle rules for old bronze prefixes; move to Intelligent-Tiering after
   90 days rather than deleting data.
3. Keep maintenance on a predictable cadence (currently every 72 cycles), or move
   it to a fixed weekly window, so object count does not spike.
4. Measure Cost Explorer before and after each change; keep the budget alert.
5. If Superset becomes multi-user, add query caching and Postgres metadata before
   adding compute.
6. At larger scale, consider S3 Tables auto-compaction, Athena capacity pricing,
   and per-workgroup cost allocation tags.

## How to check the bill

Use AWS Cost Explorer, grouped by service, for the same account and date range.
Exclude unrelated services before comparing to these figures. The budget alert is
a guardrail, not a precise forecast; a short backfill window can look alarming
without indicating steady-state cost.

The operational rule is simple: if object count rises without a corresponding
data-volume rise, run compaction before assuming storage is the problem.
