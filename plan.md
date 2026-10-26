# Build plan and as-built record

**What this answers:** how the pipeline is put together today, what was actually
built, and where the remaining work is. It is an implementation map, not the
original wish list.

**Status:** Phases 0–5 complete, including post-phase hardening through
Architecture Decision Record 030 (ADR-030). The system runs; the remaining items
are limits and scale work, not missing core pieces. Terms are in the
[glossary](docs/glossary.md).

## Current state

| Metric | Current |
|---|---|
| Symbols | 7 USDT pairs |
| Source streams | Trades + 1 Hz top-of-book quotes |
| Trade volume | ~150–200/s (~15M/day) |
| Tests | 118 offline unit tests; 1 opt-in Kafka integration test |
| dbt | 128 nodes: 14 models + 114 data tests |
| Superset | 20 charts / 10 datasets / 2 tabs |
| Decisions | 30 ADRs + `ADR-012a` addendum |
| Schedule | ~30–35 minutes per quality → dbt cycle |
| Maintenance | Every 72 cycles (~1.5–2 days) |
| Measured cost | $3.40 for 16–24 Oct 2026; ~$11–12/month run-rate |

## What changed from the original plan

| Original assumption | As built | Why |
|---|---|---|
| XRP only first | 7 coins after the single-coin path worked | The pipeline was stable enough to expand; routing was fixed in ADR-028 |
| Quality/dbt every ~10 minutes | ~30–35 minutes | A dbt build plus GX scan takes longer than 10 minutes at this volume |
| 5 initial charts | 20 charts across 2 tabs | The dashboard gained business and pipeline-health views |
| `table.upsert` for bronze | Partition-scoped key check, then append | The full-table update-or-insert did work proportional to the whole table and stalled at scale (ADR-013) |
| Trades only | Added a separate ticker stream | Trades cannot show quoted spread, depth, or imbalance (ADR-029) |
| README as the main reference | README hub + dedicated docs | Each topic needs enough depth to be useful on its own |

## Current architecture

```text
Binance WebSocket
  → producer (validate, reconnect, key by symbol)
  → Kafka trades.raw / ticker.raw
  → consumer (45 s or 20k rows)
  → bronze Iceberg on S3 + Glue
  → GX + integrity + health checks
  → dbt on Athena (staging → silver → gold)
  → Superset
```

One sentence per trust boundary:

- **Producer:** turns a WebSocket message into a validated event; never writes
  half a trade.
- **Kafka:** holds the stream for seven days and lets a consumer replay it.
- **Consumer:** writes a batch to Iceberg first, commits its Kafka offset second.
- **Quality gate:** reads the new window locally; failure stops dbt.
- **dbt:** builds typed tables and tests; a failed test stops the build.
- **Superset:** reads gold and ops tables, never raw bronze.

More detail: [architecture](docs/architecture.md) and
[one trade's journey](docs/architecture.md#one-trades-journey).

## Repository layout

| Path | Contents |
|---|---|
| `src/pipeline/producer` | Binance WebSocket trade and ticker producers |
| `src/pipeline/consumer` | Kafka consumer, batcher, bronze writer, ticker consumer, metrics |
| `src/pipeline/common` | Settings, models, logging, Iceberg catalog, retry helpers |
| `src/pipeline/quality` | GX runner, whole-table integrity, lake health |
| `src/pipeline/scheduler` | Quality → dbt → maintenance loop, dedup repair |
| `src/pipeline/backfill` | Binance 1-minute kline archive loader |
| `src/pipeline/superset` | REST dashboard provisioning |
| `dbt/models` | Staging views, silver/gold models, tests, column docs |
| `quality/suites` | Great Expectations suite snapshots |
| `infra` | OpenTofu: S3, Glue, Athena, IAM, budget, state backend |
| `superset` | Derived Superset image and config |
| `tests/unit`, `tests/integration` | Offline tests and the opt-in Kafka roundtrip |
| `docs` | Architecture, data model, testing, ADRs, runbook, cost, glossary |

## Component records

### Configuration and contracts

- `pydantic-settings` reads environment variables from `.env`; `.env.example` is
  committed and real values are not.
- `TradeEvent` validates symbol, trade ID, decimal price/quantity, UTC event time,
  UTC ingest time, and buyer-maker flag.
- Structured JSON logs include batch size, lag, maximum event time, and commit
  duration.
- The catalog is configurable: `glue` for real runs, `sql` for offline tests.

Key settings: `TICKSTREAM_SYMBOLS`, `TICKSTREAM_SYMBOL_PARTITIONS`,
`TICKSTREAM_BATCH_INTERVAL_S=45`, `TICKSTREAM_BATCH_MAX_ROWS=20000`,
`TICKSTREAM_FRESHNESS_MAX_LAG_S=300`, `TICKSTREAM_MAINTENANCE_EVERY_N_RUNS=72`.

### Producers

- One asyncio producer holds Binance's combined trade stream; a second holds the
  ticker stream.
- Messages are validated with pydantic, stamped with ingest time, and sent with
  `acks=all` (wait for the broker's acknowledgement), producer idempotence (a
  retried send is not stored twice), compressed messages, and `key=symbol`.
- Reconnect uses exponential backoff with jitter. A 60-second no-message watchdog
  treats a silent-but-open socket as dead (ADR-025).
- SIGTERM flushes and closes cleanly.

### Consumers

- Three trade consumers share the `bronze-writer` group, one per partition.
  Symbols are pinned to partitions so each symbol has one writer (ADR-028).
- One ticker consumer writes the lower-volume quote stream.
- Manual commits, `auto.offset.reset=earliest` (replay from the oldest message if
  there is no saved position), cooperative-sticky assignment (rebalances move only
  the partitions that need to move), and commit-on-revoke.
- A batch flushes at 45 seconds or 20,000 rows. The next window starts only after
  write + commit + best-effort metrics, so a slow step cannot create one-row
  batches (ADR-014/030).
- Before the dedup scan, the consumer refreshes its Iceberg table handle so it
  sees writes from other replicas (ADR-015).
- Bronze merge: scan only the batch's day/symbol partitions, check which
  `(symbol, trade_id)` keys already exist, and append only new rows. A full replay
  appends zero rows (ADR-013).

### Quality and scheduler

- Great Expectations (GX) validates the last successful window minus five minutes: required columns,
  key nulls, positive price/quantity, unique trade keys, at least one row, and
  freshness ≤ 300 seconds.
- A failed suite writes evidence and blocks dbt; the quality window does not
  advance.
- Every 72 cycles: whole-table duplicate check over bronze keys, per-layer row
  counts, per-symbol freshness, and `OPTIMIZE` + `VACUUM` on the five core tables.
- The scheduler has `--once` style targets for manual quality, integrity, health,
  dbt, and maintenance runs.

### dbt models

- **Staging:** thin views that cast and normalise bronze.
- **Silver:** `silver_trades` and `silver_tickers` are incremental merges with a
  two-hour lookback; `silver_klines` is a view over the archive.
- **Gold:** `gold_candles_1m` has OHLC, volume, USD notional, VWAP, trade count,
  and volatility. Daily/hourly summaries share one macro. Microstructure comes
  from ticker data. The unified candle view prefers live candles over archive
  candles.
- Incremental windows anchor on the target table, not the source, so a skipped
  run catches up on the next success (ADR-024).
- `update_condition` skips unchanged rows, avoiding Glue timeouts (ADR-009).
- Every model has data tests, and the core columns are documented.

### Infrastructure and serving

- OpenTofu state is in S3 with a DynamoDB lock; bootstrap is a one-time command.
- In the main stack, the $20 budget alert is declared before S3, Glue, the Athena
  workgroup, and a scoped IAM user. The state bucket and lock table are created
  earlier by the one-time bootstrap. Declaration order does not guarantee
  OpenTofu's parallel execution order.
- No costly resources: no Redshift, no large instances.
- Superset uses a derived image with `pyathena[pandas]`, SQLite metadata on a
  volume, and a REST provisioning script. Datasets point at gold and ops tables.
- The dashboard is reproducible: create/update database → datasets → charts →
  dashboard, with stable IDs and labels.

## Current versions

| Component | Version |
|---|---|
| Python | 3.12 |
| Apache Kafka | 4.2.0 |
| Apache Superset | 6.0.0 |
| PyIceberg | 0.12.0 |
| `confluent-kafka` | 2.15.1 |
| dbt-core | 1.12.5 |
| `dbt-athena-community` | 1.11.1 |
| Great Expectations | 1.23.2 |
| PyArrow | 25.0.1 |
| PyAthena | 3.34.0 |

## Phases and evidence

| Phase | What was delivered | Evidence |
|---|---|---|
| 0 — Skeleton + infra | uv project, lint/types/tests, Makefile, settings, remote state, budget, S3, Glue, Athena, IAM | `make lint typecheck test`; plan reviewed before apply |
| 1 — XRP ingestion | Producer, Kafka, consumer, bronze, replay tests | Restart/SIGKILL: no duplicates; offsets resumed |
| 2 — Models + quality | dbt silver/gold, GX, scheduler, maintenance | GX failure blocked dbt; OPTIMIZE/VACUUM ran |
| 3 — Dashboard + expansion | Superset, 7 symbols, parallel consumers | 20 charts / 10 datasets; 7 symbols ingested |
| 4 — Docs + hardening | ADRs, runbook, cost, ticker, backfill, integrity | 30 ADRs; 846,720 archive rows; 118 tests |
| 5 — CI | Lint, types, offline tests on push/PR | GitHub Actions run 40213500712 green |

## Decisions map

All non-obvious choices are recorded in the [ADR index](docs/decisions.md).
Start with:

- **Reliability:** ADR-001, 013, 014, 015, 016, 024, 030.
- **Scale and routing:** ADR-004, 006, 028, 029.
- **Quality and data shape:** ADR-002, 008, 009, 017, 019, 021.
- **Serving:** ADR-011, 012, 012a, 018, 020, 022, 027.
- **Recovery:** ADR-023, 025, 026.

The index shows each decision's status; superseded decisions are kept rather than
rewritten.

## Risks and limits

| Risk | Current mitigation | Remaining exposure |
|---|---|---|
| Single broker / host sleep | Kafka volume and offsets survive a restart | Full outage until the host returns; trade and ticker history from the gap is lost |
| Gold is ~30–35 minutes stale | Freshness is measured and charted | Not suitable for live trading |
| Small files raise S3 request cost | Batched writes, OPTIMIZE, VACUUM | No expiration/tiering lifecycle yet; only abort-incomplete-upload exists |
| A duplicate outside the GX window | Whole-table integrity every 72 cycles | Detection can take up to one maintenance interval |
| PyIceberg partitioned upsert bug | Anti-join merge avoids it; replay test | A future PyIceberg upgrade needs the same test |
| CI cannot prove the live path | Manual live checks documented in the runbook | dbt/GX/Kafka integration is not continuous |
| Kafka replay is limited | Seven-day retention | Trades and quotes outside retention are lost; archive backfill restores candles only |

## Next work

1. Apply S3 lifecycle rules for `athena-results/` and old snapshots; measure the
   request-cost change.
2. Keep maintenance on a predictable cadence (currently every 72 cycles); consider
   a fixed weekly window if changing it.
3. Add freshness and lag alerts if this moves beyond a portfolio setup.
4. At larger scale: 3 Kafka brokers (three copies of each partition), managed
   compaction, Postgres for Superset metadata, and IAM roles instead of a user key.
5. Keep documentation, counts, cadence, and ADR index in sync with any change.

## How to work on this repo

```bash
make install
make lint typecheck test
make up
make quality
make dbt-build
make down
```

The Makefile is the entry point. Read the [runbook](docs/runbook.md) before
running infrastructure commands; `make infra-apply` and `tofu destroy` require
explicit human approval. Do not reset Kafka offsets or delete data to fix an
incident without checking the runbook first.
