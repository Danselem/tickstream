# tickstream

Real-time crypto streaming pipeline (portfolio project): live Binance trades and
top-of-book snapshots → Kafka → Apache Iceberg lake on AWS S3 → dbt models on
Athena → Superset dashboards, with Great Expectations quality gates at every
step.

**Status (Oct 2026): end to end live** — 7 coins (BTC, ETH, SOL, ADA, DOGE, LINK,
AVAX), a 1 Hz `@ticker` stream for quoted spread/depth, GX-gated dbt builds, a
published 20-chart Superset dashboard, 118 unit tests and 128 dbt nodes; ~$3.4
AWS spend for 16–24 Oct against a $20 budget alert.

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

`make help` lists everything: `quality`, `dbt-build`, `maintenance`, `backfill`,
`integrity` (whole-table duplicate check), `superset` (re-provision dashboard),
`logs`, infra targets.

## Architecture

```mermaid
flowchart LR
  B[Binance WS] -->|trades| P[producer] --> K[(trades.raw)] --> C[consumer ×3]
  C -->|merge on symbol, trade_id| BR[(bronze_trades)]
  B -->|@ticker 1 Hz| TP[ticker-producer] --> TK[(ticker.raw)] --> TC[ticker-consumer]
  TC -->|merge on symbol, event_time| BT[(bronze_tickers)]
  BR --> GX[GX gate<br/>local PyIceberg scan]
  BT --> GX
  GX -->|pass| DBT[dbt on Athena<br/>staging → silver → gold]
  GX -->|fail| X[block dbt + ops report]
  DBT --> G[gold: candles, microstructure,<br/>market summaries]
  G --> S[Superset<br/>20 charts, 2 tabs]
  K -.->|every N cycles| M[Athena OPTIMIZE + VACUUM]
  DBT -.-> M
```

Binance archives supply 1m klines for history (`bronze_klines`); the unified
candle view prefers live candles per minute and falls back to klines. See
`docs/architecture.md` for the full diagram and `docs/decisions.md` for ADR-001…029.

## How it works

- **Ingest.** Two WebSocket producers (one per stream family) write
  `trades.raw` and `ticker.raw`, keyed by symbol. Trades use explicit
  symbol→partition routing (ADR-028) — the default hash put 74% of volume on one
  partition.
- **Bronze.** Consumers micro-batch (45 s / 20k rows), prune the batch's
  day×symbol partitions, anti-join keys in Arrow, and append only new rows.
  Offsets commit only after the Iceberg write; replays append nothing
  (ADR-013/014/015). Trades dedupe on `(symbol, trade_id)`, tickers on
  `(symbol, event_time)`.
- **Quality.** GX validates a local PyIceberg scan (zero Athena spend) for
  trades and tickers; a failing suite blocks dbt and records the failure in
  `quality_results` (ADR-002/008).
- **Transform.** dbt on Athena: staging views → silver merges → gold 1m OHLC,
  VWAP, notional, trade count, volatility, daily/hourly summaries, and
  1-minute microstructure (spread bps, mid, displayed depth, imbalance). The
  `gold_candles_1m_unified` view merges live candles with archive klines
  (ADR-026). Incremental windows are target-anchored and overridable (ADR-024).
- **Serve.** Superset 6, provisioned entirely through its REST API: 10 datasets
  and 20 charts across **Market** and **Pipeline health**, with native Symbol
  (BTC default) and Time filters and 24 h detail / 30 d trend windows
  (ADR-018/020/021/022/027).
- **Maintain.** The scheduler loop runs quality → dbt build (~12 min for 128
  nodes) then sleeps 10 min; every N cycles it runs Athena `OPTIMIZE` +
  `VACUUM`, and a whole-table duplicate check every cycle (ADR-016).

All timestamps are UTC (event time and ingest time are both kept).

## Repo map

```
src/pipeline/{producer,consumer,common,quality,scheduler,superset,backfill}/  typed Python
dbt/models/{staging,silver,gold}/   views/incremental merges + tests + docs
quality/suites/                     GX suite snapshots (reports/ gitignored)
superset/   image + config + bootstrap + REST provisioning (dashboard as code)
infra/      OpenTofu: budget alert first, then bucket/Glue/Athena/IAM
tests/      pytest: 118 unit (incl. replay idempotency) + Kafka roundtrip
docs/       architecture, decisions (29 ADRs), cost (with actuals), runbook
```

## Verification highlights

- Restart + SIGKILL mid-stream: 0 duplicates (replay test).
- Cross-replica replay (new owner sees the old owner's commit): 0 duplicates.
- Live rebalance (scale 3→2→3): 0 new duplicates; `partitions_revoked` logged.
- GX failure injection blocks dbt (exit 1, window not advanced); whole-table
  integrity check also blocks dbt when duplicates > 0 (ADR-016).
- Second dbt build merges new keys only (silver 0 dupes, gold candles stable).
- 20k-row stale replay batch: 2 appended, 19,998 skipped (overlap absorbed).
- Maintenance: bronze 348 → 4 files, 0 dupes; one-time dedup removed 15.8k rows.
- Backfill: 846,720 archive kline rows (7 coins × Aug 1–Oct 23), checksum-verified.
- Dashboard: SQL Lab returns live Athena rows; 20 charts / 10 datasets; ticker
  spread and imbalance charts scoped by the Symbol filter; XRP excluded from
  filter values while remaining queryable.
- Kafka lag after the partition fix: 440k → ~0; ticker lag bounded (~100–500)
  after the batch-window fix (metrics no longer postpone the next window).

## Cost (16–24 Oct actuals)

S3 ≈ **$3.28** · Athena ≈ **$0.12** · Glue ≈ $0 · budget alert $20/mo.
The lake was **8.9 GB / 59.7k objects** before maintenance and **6.9 GB /
55.8k objects** after `OPTIMIZE` + `VACUUM`; storage itself is cents — requests
from small files dominate. GX and integrity checks run locally, so quality adds
zero Athena spend. Details and the small-file plan: `docs/cost.md`.

## Key design decisions

- At-least-once delivery with idempotent bronze merges; offsets only after the
  write (ADR-013/014/015).
- GX on a local Iceberg scan, not Athena: correctness for free (ADR-002).
- Separate ops tables in the same Glue database; health checks never block data
  on the first failure (ADR-008/021).
- Archive backfill + unified candle view so charts keep history and prefer live
  data for overlapping minutes (ADR-026/027).
- Explicit symbol→partition routing and a separate ticker stream to isolate the
  hot path (ADR-028/029).

## What I'd change at scale

- Kafka: 3+ brokers, RF=3, more partitions; consumer group with >1 member.
- Bronze: S3 Tables auto-compaction or Flink-style streaming merge; smaller
  `BATCH_MAX_ROWS` if single-batch latency matters more than file count.
- Orchestration: Airflow/Dagster over the bespoke scheduler loop once DAGs grow.
- Serving: Superset with Postgres metadata + caching; Athena provisioned capacity
  if dashboards go concurrent.
- Schema: full schema-evolution drills (add-nullable-column end to end).
- Multi-region/auth: IAM roles for service accounts instead of user keys.

## Known gaps

- A host stop/sleep (Docker daemon down) pauses all services. Trades and candles
  recover from Binance archives T+1 (`make backfill`); ticker history cannot be
  replayed — Binance publishes no historical top-of-book snapshots.
- Gold freshness follows the scheduler cadence (up to ~20–25 min); the 24 h
  chart is complete via the unified view once klines are backfilled.
- `pipeline_metrics` compaction can fail while four consumers commit to it;
  retry during a quiet window (data is unaffected).

Decisions behind all of this: `docs/decisions.md`. Ops: `docs/runbook.md`.
