# tickstream

Real-time crypto streaming pipeline (portfolio project): live Binance trades →
Kafka → Apache Iceberg lake on AWS S3 → dbt models on Athena → Superset
dashboards, with Great Expectations quality gates at every step.

**Status (Oct 2026): end to end live** — 7 coins (BTC, ETH, SOL, ADA, DOGE, LINK,
AVAX), GX-gated dbt builds, published Superset dashboard, ~$0.62 AWS spend for
two weeks against a $20 budget alert.

## Stack

Python 3.12 · uv · Docker Compose · Apache Kafka (KRaft, 1 broker) · PyIceberg ·
AWS S3 + Glue + Athena · dbt (dbt-athena) · Great Expectations · OpenTofu ·
Apache Superset. Out of scope by design: Spark, Airflow, trading logic, ML.

## Quickstart

```bash
cp .env.example .env   # fill in: AWS keys, bucket, Superset secret + admin
make install           # uv sync
make lint typecheck test
make infra-bootstrap   # one-time tofu state backend
make infra-plan ALERT_EMAIL=you@example.com   # review
make infra-apply       # explicit human approval required
make up                # all services; dashboard at http://localhost:8088
```

`make help` lists everything: `quality`, `dbt-build`, `maintenance`, `superset`
(re-provision dashboard), `logs`, infra targets.

## How it works

```
Binance WS (combined stream, 7 coins) ──producer──▶ Kafka trades.raw (key=symbol)
  ──consumer (45 s / 20k-row micro-batches)──▶ S3 Iceberg bronze (Glue catalog)
scheduler loop (10 min): GX check (local PyIceberg scan) ──pass──▶ dbt build
                         └──fail──▶ block dbt, report to tickstream.quality_results
daily-ish: Athena OPTIMIZE + VACUUM (bronze/silver/gold)
Superset ──pyathena──▶ Athena (gold candles, ops health tables)
```

Delivery is at-least-once; processing is idempotent. Bronze merges on
`(symbol, trade_id)` via a partition-pruned scan + Arrow anti-join + append-new
(ADR-013; the naive `table.upsert` proved O(table) per batch). Kafka offsets
commit only after the Iceberg write; transient S3/Glue failures retry with
backoff (tenacity), metrics appends are best-effort. Silver re-dedupes as
defense-in-depth; gold builds 1-min OHLC + volume + VWAP + trade count +
log-return volatility. All timestamps UTC (event time + ingest time kept).

## Repo map

```
src/pipeline/{producer,consumer,common,quality,scheduler,superset}/  typed Python
dbt/models/{staging,silver,gold}/   incremental merges + tests + docs
quality/suites/bronze_suite.json    GX suite snapshot (reports/ gitignored)
superset/   image + config + bootstrap + REST provisioning (dashboard as code)
infra/      OpenTofu: budget alert first, then bucket/Glue/Athena/IAM
scheduler/  (in src) quality → dbt → maintenance loop
tests/      pytest, 66 unit + integration (Kafka roundtrip, replay idempotency)
docs/       architecture, decisions (13 ADRs), cost (with actuals), runbook
```

## Verification highlights

- Restart + SIGKILL mid-stream: 0 duplicates (replay integration test).
- GX failure injection blocks dbt (exit 1, window not advanced).
- Second dbt build merges new keys only (silver 0 dupes, gold candles stable).
- 20k-row stale replay batch: 2 appended, 19,998 skipped (overlap absorbed).
- Maintenance: bronze 348 → 4 files, 0 dupes.
- Dashboard: SQL Lab through Superset returns live Athena rows.

## Cost (Oct 1–15 actuals)

S3 ≈ $0.61 (481 MB, 11k objects) · Athena ≈ **$0.01** · budget alert $20/mo.
Details + recommendations: `docs/cost.md`.

## What I'd change at scale

- Kafka: 3+ brokers, RF=3, more partitions; consumer group with >1 member.
- Bronze: S3 Tables auto-compaction or Flink-style streaming merge; smaller
  `BATCH_MAX_ROWS` if single-batch latency matters more than file count.
- Orchestration: Airflow/Dagster over the bespoke scheduler loop once DAGs grow.
- Serving: Superset with Postgres metadata + caching; Athena provisioned capacity
  if dashboards go concurrent.
- Schema: full schema-evolution drills (add-nullable-column end to end).
- Multi-region/auth: IAM roles for service accounts instead of user keys.

Decisions behind all of this: `docs/decisions.md`. Ops: `docs/runbook.md`.
