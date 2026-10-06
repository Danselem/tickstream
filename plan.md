# Build Plan — `tickstream` (Binance → Kafka → Iceberg → dbt → Superset)

Status: **for review**. Nothing is built yet. Phase 0 starts only after this plan is approved.

Locked user decisions: (1) bronze idempotency = upsert/MERGE on `(symbol, trade_id)`;
(2) Great Expectations validates a local PyIceberg scan, not Athena; (3) OpenTofu state in
S3 + DynamoDB with a one-time bootstrap; (4) local compose always targets real
S3/Glue/Athena; pytest covers offline via SQL catalog + temp warehouse.

## 0. Verified facts (researched Oct 2026)

| Area | Verified finding |
|---|---|
| Binance WS | Spot market streams: `wss://stream.binance.com:9443` (alt `:443`), raw `/ws/<stream>`, combined `/stream?streams=...`; market-data-only mirror `wss://data-stream.binance.vision` (no auth, recommended for market data). Stream name lowercase: `xrpusdt@trade`. Payload: `e,E,s,t,p,q,T,m,M` (trade_id=`t`, price=`p`, qty=`q`, event time=`T` ms, buyer-is-maker=`m`). Handle server ping/pong and 24 h reconnects. Endpoint kept in config so it's swappable. |
| Kafka client | `confluent-kafka` 2.15.0 (Jun 2026, librdkafka) — actively maintained; idempotent producer, manual-commit consumer. Chosen over revived `kafka-python`. |
| Kafka image | Official `apache/kafka` (4.2/4.3) image, KRaft combined single node; RF=1 settings (`OFFSET_TOPIC_REPLICATION_FACTOR=1` etc.). |
| Iceberg | `pyiceberg` 0.12.0 (Sep 2026) with Glue catalog + `table.upsert(df, join_cols=[...])` — the MERGE primitive for bronze. Known recent issue #3758 ("upsert duplicates rows on partitioned table", Aug 2026) → **mandatory replay-dedup integration test**; fallback documented in §4.3. |
| dbt | `dbt-athena-community` 1.11.1 (Sep 2026) + dbt-core 1.10/1.12; Iceberg `incremental_strategy='merge'` with `unique_key` on Athena engine v3; `OPTIMIZE`/`VACUUM` are GA on Athena engine v3. Pin `pyathena<=3.35` (documented dbt dependency note). Exact `partitioned_by` syntax for `day()+identity` verified against adapter docs during Phase 2. |
| Great Expectations | GX Core 1.23.2 (Sep 2026), fluent API (datasource → asset → batch definition → suite → checkpoint). Validates a **local PyIceberg scan** (free, no Athena cost): pandas datasource + `batch_parameters={"dataframe": ...}` fed from `table.scan(window).to_pandas()`. |
| Superset | Needs `pyathena[pandas]`; connection `awsathena+rest://...` (no Java). Derived image (`FROM apache/superset`) installs the driver. |
| Sizing (goes in `docs/decisions.md`) | ~200 trades/s avg → ~17M events/day; bronze Parquet ≈ 1–2 GB/day; 60 s micro-batch ≈ 12k rows/commit; 30-day retention ≈ tens of GB (pennies on S3). All Athena queries partition-pruned → cents/day. Measure actuals later. |

## 1. Architecture

```
Binance WS ──producer──▶ Kafka(trades.raw, key=symbol) ──consumer──▶ S3 Iceberg bronze
                                                                     (upsert, 30–60s micro-batch, commit offsets AFTER write)
scheduler loop (every ~10 min):  GX check (local PyIceberg scan) ──pass──▶ dbt build (Athena: silver→gold)
                                 └──fail──▶ block dbt, write failure report to ops tables
daily-ish:  ALTER TABLE ... OPTIMIZE / VACUUM (Athena engine v3)
Superset ──pyathena──▶ Athena (silver, gold, ops health tables)
```

**Delivery semantics:** at-least-once from Kafka; idempotency via bronze upsert;
offsets committed only after a successful Iceberg commit; crash → replay → upsert absorbs duplicates.

**"Bronze raw" definition (ADR):** no transformations/enrichment of event content; the
upsert is a dedup-commit, not a modification of trade facts. Recorded in `docs/decisions.md`.

## 2. Repo layout (files to create)

```
pyproject.toml, uv.lock, .env.example, Makefile, .dockerignore, Dockerfile
src/pipeline/
  common/      config.py (pydantic-settings), logging.py (structlog JSON), models.py
               (pydantic TradeEvent/BatchReport), iceberg.py (catalog/table bootstrap), aws.py
  producer/    binance_ws.py, kafka_producer.py, run.py        (websockets + backoff)
  consumer/    kafka_consumer.py, batcher.py, bronze_writer.py, metrics.py, run.py
  quality/     gx_runner.py (builds scan window, runs GX checkpoint, persists results)
  scheduler/   run.py (loop: quality → dbt build → maintenance every N cycles; --once mode)
dbt/           dbt_project.yml, profiles.yml.example, models/{staging,silver,gold}/... + schema.yml
quality/       gx.yml, suites/*.json (bronze suite)
infra/         bootstrap/ (S3+DynamoDB state), main.tf, budget.tf, s3.tf, glue.tf, athena.tf, iam.tf, variables.tf, outputs.tf
superset/      Dockerfile, superset_config.py, bootstrap.sh (db upgrade, admin, athena db)
docker-compose.yml
tests/unit/    payload fixtures, model/batcher/config/dedup tests
tests/integration/  replay-idempotency, produce→consume roundtrip (auto-skip without env)
docs/          architecture.md (mermaid), decisions.md (ADRs), cost.md, runbook.md
```

**Open layout question (needs your sign-off):** AGENTS.md shows a top-level `scheduler/`
directory; this plan puts scheduler code in `src/pipeline/scheduler/` so everything stays
importable/typed/testable under one `uv` package (`python -m pipeline.scheduler`). Compose
runs the module — no functional difference.

## 3. Component specs

### 3.1 `common` — configuration & contracts
- `Settings` (pydantic-settings): `AWS_REGION`, `S3_BUCKET`, `GLUE_DATABASE`,
  `CATALOG_TYPE` (glue|sql for tests), `KAFKA_BOOTSTRAP`, `TOPIC=trades.raw`,
  `SYMBOLS=["XRPUSDT"]`, `BINANCE_WS_URL`, `BATCH_INTERVAL_S=45`, `BATCH_MAX_ROWS=20_000`,
  `FRESHNESS_MAX_LAG_S=300`, `QUALITY_INTERVAL_S=600`, `MAINTENANCE_EVERY_N_RUNS=72`,
  `LOG_LEVEL`. Committed `.env.example`, real `.env` gitignored.
- `TradeEvent` pydantic model: `symbol, trade_id, price: Decimal, quantity: Decimal`,
  `event_time: datetime` (ms→UTC), `ingest_time: datetime` (UTC), `is_buyer_maker: bool` —
  strict validation; malformed messages are counted + logged, never written.
- structlog JSON logging everywhere; every consumer cycle logs `lag`, `batch_rows`,
  `batch_max_event_time`, `commit_ms`.
- Full type hints on all functions; ruff + mypy enforced via `make`.

### 3.2 Producer (`websockets`, asyncio)
- Connect to `wss://.../stream?streams=xrpusdt@trade` (combined stream, ready for
  multi-coin), pydantic parse, stamp `ingest_time`, produce with `acks=all`,
  `enable.idempotence=true`, `compression=zstd`, key=`symbol`.
- Reconnect with exponential backoff + jitter; lib handles ping/pong;
  SIGTERM → flush → close.

### 3.3 Consumer
- `confluent-kafka` Consumer: `enable.auto.commit=false`, `auto.offset.reset=earliest`,
  group `bronze-writer`.
- Batcher: flush on `BATCH_INTERVAL_S` **or** `BATCH_MAX_ROWS`; parse → pyarrow →
  `table.upsert(batch, join_cols=["symbol","trade_id"])` → **then** `commit(asynchronous=False)`.
- On write failure: retry with backoff; never commit → replay safe. Idempotent table
  bootstrap at startup (`create_table_if_not_exists`, partition spec `day(event_time)` +
  `identity(symbol)`, format v2).
- Lag tracking (high-watermark − position) + freshness logged each cycle and appended to
  `ops.pipeline_metrics`.
- **Guardrail:** integration test replays the same batch twice and asserts row count
  unchanged (covers PyIceberg issue #3758). If it fires: fallback =
  `table.overwrite(batch, predicate=...)`, or pin `pyiceberg==0.11.1`.

### 3.4 Quality (GX 1.23, local scan)
- Suite on bronze window (`event_time >= last_successful_run − 5 min` overlap):
  required columns/no nulls in key fields, `price > 0`, `quantity > 0`, `trade_id`
  uniqueness within window, freshness `max(event_time)` ≤ threshold.
- Runner writes pass/fail + metric summary (JSON artifact in `quality/reports/`,
  gitignored) **and** appends a row to `ops.quality_results` for the Superset health panel.
- Failure → exit nonzero → scheduler skips dbt this cycle (Brief §6).

### 3.5 Scheduler (replaces Airflow)
- Single loop: every `QUALITY_INTERVAL_S`: GX → (pass?) → `dbt build` → log; every
  `MAINTENANCE_EVERY_N_RUNS`: `OPTIMIZE <table> REWRITE DATA` + `VACUUM <table>` on
  bronze/silver/gold (Athena via pyathena; needs `s3:DeleteObject` for VACUUM).
- `--once` mode backs `make quality` and `make dbt-build`.

### 3.6 dbt (Athena engine v3, Iceberg)
- **staging:** thin views over bronze (cast, UTC, no logic).
- **silver `silver_trades`:** `incremental`, `merge`, `unique_key=["symbol","trade_id"]`,
  dedup via `row_number()`, `is_incremental()` refresh window = last 2 h of `event_time`
  (late-arrival grace, idempotent re-merge), partitioned day+symbol.
- **gold `gold_candles_1m`:** 1-min OHLC, `volume`, `vwap` (Σp·q/Σq), `trade_count`,
  `volatility` (stddev of per-trade log returns within minute), `merge` on
  `(symbol, candle_start)`, trailing 30-min recompute window.
- Tests: `unique`+`not_null` on keys, `accepted_values`, `dbt_utils.expression_is_true`
  (price/qty>0), source freshness; all columns documented; Superset exposure declared.
- Runs only after GX passes (enforced by scheduler; documented for manual runs).

### 3.7 Infra (OpenTofu)
- `make infra-bootstrap`: one-time state bucket + DynamoDB lock table; then S3 backend in `infra/`.
- **Creation order: budget alert (email var) first**, then: S3 bucket (SSE-S3,
  public-access block, abort-incomplete-MPU lifecycle), Glue DB, Athena workgroup
  (engine v3, results prefix), IAM user + policy scoped to bucket ARN/prefix, Glue DB,
  workgroup only.
- `make infra-plan` output reviewed before any apply. **No `make infra-apply` or
  `tofu destroy` without explicit human approval.** `tofu destroy` documented in runbook only.

### 3.8 Superset
- Single `apache/superset` derived image (driver install + `superset_config.py`); SQLite
  metadata; init: `db upgrade` + create admin from env; datasets point at gold/ops tables
  (never raw bronze for dashboards).
- Dashboard: price/candle chart, volume+VWAP, volatility, pipeline-health (freshness, row
  counts, last GX result) from `ops.*`.

### 3.9 Compose & Make
- Services: `kafka` (official image, KRaft RF=1), `producer`, `consumer`, `scheduler`,
  `superset`. One multi-stage uv Dockerfile for the three Python services; healthcheck on
  Kafka; `.dockerignore`.
- Makefile targets exactly per AGENTS: `install up down lint format typecheck test quality
  dbt-build infra-bootstrap infra-plan infra-apply` (+ `logs`, `restart` convenience).

## 4. Phases (finish + verify one before the next; small commits with tests)

| Phase | Deliverables | Done when |
|---|---|---|
| **0 – Skeleton + infra** | uv project, ruff/mypy/pytest wired, Makefile, `.env.example`, `common` (Settings/logging/models), OpenTofu (bootstrap → budget alert → resources), docs stubs | `make lint typecheck test` green; `make infra-plan` reviewed → apply approved; budget alert confirmed in console |
| **1 – XRP ingestion** | Kafka service, producer, consumer, bronze table, unit + replay-idempotency tests | XRP trades land in bronze via `make up`; restart mid-stream creates no duplicates; offsets resume; lag/freshness logged |
| **2 – Models + quality + schedule** | dbt staging/silver/gold + tests/docs, GX suite, scheduler loop, OPTIMIZE/VACUUM cadence, ops tables | `make quality` → `make dbt-build` green; simulated GX failure blocks dbt; second loop run merges without dupes |
| **3 – Dashboard + expansion** | Superset service + dashboard, then 6–7 coins via `SYMBOLS` config (combined stream, key=symbol) | Dashboard renders from Athena; multi-coin data correct per symbol |
| **4 – Docs** | README (diagram, setup, decisions, cost, "what I'd change at scale"), `docs/*` | Brief §11 definition-of-done fully checked |

## 5. Risks to watch (each with a verification step)

1. PyIceberg partitioned-upsert regression → replay test in Phase 1; fallback =
   overwrite-predicate or pin 0.11.1.
2. dbt-athena `partitioned_by` day+identity syntax / pyathena pin → verified when models
   first compile (Phase 2).
3. Binance endpoint reachability from your region → endpoint is config;
   `data-stream.binance.vision` mirror ready.
4. Single-broker KRaft = SPOF (accepted, documented as intentional MVP tradeoff in
   `docs/decisions.md`).

## 6. Explicitly out of scope

Spark, Airflow, equities, trading/ML, second exchange, schema registry, multi-broker
Kafka, MinIO/offline compose — all deferred per Brief/AGENTS.

## 7. Next step

Approve this plan (including the §2 layout question). Phase 0 then starts with the repo
skeleton + `make infra-plan` output shown before any apply.
