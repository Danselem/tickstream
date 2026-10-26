# tickstream — Project Brief

**What this is:** a working data pipeline that turns live Binance trades and
1-second price quotes into checked, minute-level tables and a dashboard. It runs
locally in Docker, stores data in AWS, and is built so a crash or replay does not
create duplicate records.

**What this is not:** a trading system, a price predictor, or a production
multi-tenant service. It is a portfolio project that shows the engineering
decisions behind a small lakehouse.

## 1. Goal

Build one end-to-end path — source → queue → storage → checks → models →
dashboard — and be able to explain every non-obvious choice with evidence.

Success means a reader can answer three questions:

1. What happens to one trade from Binance to a chart?
2. What prevents losses and duplicates when something crashes?
3. What does it cost, what can fail, and what would change at larger scale?

## 2. Current scope

| Item | Current state |
|---|---|
| Source | Binance Spot public market data (no account or API key) |
| Symbols | 7 USDT pairs: `BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `ADAUSDT`, `DOGEUSDT`, `LINKUSDT`, `AVAXUSDT` |
| Streams | Trades (`@trade`) and 1 Hz top-of-book quotes (`@ticker`) |
| Volume | ~150–200 trades/s (~15M/day), plus ~5–7 quote messages/s |
| Cadence | Quality → dbt → dashboard every ~30–35 minutes; maintenance every 72 cycles |
| Freshness | Gold tables ≤ ~35 minutes; ticker data ≤ ~1 minute |
| Storage | Apache Iceberg tables on Amazon S3, registered in AWS Glue |
| Serving | Apache Superset: 20 charts, 10 datasets, two tabs |
| Monthly cost | ~$11–12 at current volume; $20 budget alert |

The project started with XRP only. It now runs the seven pairs above; XRP remains
queryable in old data but is no longer configured.

**Out of scope by design:** Spark, Airflow, equities, trading logic, machine
learning, a second exchange, a full order book, and multi-tenant security.

## 3. Architecture

```text
Binance WebSocket
  → Python producer
  → Kafka
  → Python consumer (PyIceberg)
  → bronze Iceberg on S3 + Glue
  → Great Expectations checks
  → dbt on Athena (silver, gold)
  → Superset
```

Everything except S3, Glue, Athena, and the budget alert runs locally in Docker
Compose. AWS resources are created with OpenTofu.

Two ideas make the flow safe:

- **Kafka may deliver a message twice.** The consumers therefore write with
  idempotent keys: the same `(symbol, trade_id)` is stored once.
- **A row is only marked as done after it is stored.** Consumer offsets are
  committed after the Iceberg write, so a crash replays work instead of losing it.

## 4. Stack

| Layer | Tool | What it does here |
|---|---|---|
| Source | Binance WebSocket | Pushes trades and quotes in real time |
| Ingestion | Python 3.12 producer | Validates messages, reconnects, writes to Kafka |
| Queue | Apache Kafka 4.2 (KRaft, one broker) | Buffers streams and allows replay |
| Landing | Python consumer + PyIceberg | Groups events into batches and writes bronze Iceberg |
| Lake | Amazon S3 + Apache Iceberg | Stores open-format data files and table metadata |
| Catalog | AWS Glue Data Catalog | Tells Athena where tables and schemas live |
| Warehouse | Amazon Athena | Runs SQL models and dashboard queries |
| Transformation | dbt-core + dbt-athena | Builds typed, tested silver and gold models |
| Quality | Great Expectations | Blocks dbt when the recent bronze data is bad |
| Scheduling | Python loop in a container | Runs quality, dbt, integrity, health, and maintenance |
| Infrastructure | OpenTofu | Creates S3, Glue, Athena, IAM, and the budget alert |
| Serving | Apache Superset 6.0 | Hosts the Market and Pipeline health dashboards |

No queue, table format, or BI tool is added without updating the
[ADRs](docs/decisions.md). Terms such as *Iceberg*, *offset*, and *VWAP* are
defined in the [glossary](docs/glossary.md).

## 5. Data model

Three refinement levels, explained in full in the [data model](docs/data-model.md):

| Layer | Plain meaning | Examples |
|---|---|---|
| **Bronze** | Raw facts, no business enrichment. Replays are absorbed by keys. | `bronze_trades`, `bronze_tickers`, `bronze_klines` |
| **Silver** | Typed, deduplicated, UTC-normalised rows. | `silver_trades`, `silver_tickers`, `silver_klines` |
| **Gold** | Minute-level answers for charts. | `gold_candles_1m`, summaries, microstructure |

The main key is `(symbol, trade_id)` for trades and `(symbol, event_time)` for
quotes. Both event time and ingest time are kept, so pipeline delay can be
separated from market timing.

The unified candle view prefers live candles and lets the archive fill gaps. The
default backfill starts at the first day of the month 60 days ago; the measured
load covered 84 days (846,720 rows), and the dashboard uses at least 30 days.

## 6. Data quality

Before each scheduled dbt build, Great Expectations checks the recent bronze window (with
a five-minute overlap):

- required columns exist and key fields are not null;
- price and quantity are positive;
- trade keys are unique within the window;
- at least one row arrived;
- the newest event is no older than 300 seconds.

A failure stops the cycle: dbt does not run and the quality window does not
advance. Results are summarized in `tickstream.quality_results`; manual runs also
write files under `quality/reports/`.

Two slower checks run every 72 cycles:

- **Whole-table integrity:** count duplicate trade keys across all bronze rows.
- **Lake health:** row counts per layer and freshness per symbol.

The difference matters: the frequent check is cheap but only sees a recent
window; the slower check can catch an old duplicate the window missed.

## 7. Dashboard

One published Superset dashboard, `tickstream`, with two tabs:

| Tab | Answers | Examples |
|---|---|---|
| **Market** | What is the market doing? | Candlestick, price/VWAP, volume, USD notional, trades/min, volatility, spread, book imbalance, summaries |
| **Pipeline health** | Is the pipeline healthy? | Lag, batch rows, quality runs, bronze integrity, rows per layer, freshness per symbol |

Ten datasets: gold and ops/health tables only, never raw bronze. The Symbol
filter defaults to BTC; the Time range filter applies to the dashboard.

## 8. Build phases

| Phase | Outcome | Evidence |
|---|---|---|
| 0 — Skeleton and AWS | Repo, tests, Makefile, remote state, budget alert, S3, Glue, Athena, IAM | `make lint typecheck test`; plan reviewed before apply |
| 1 — XRP ingestion | Live trades into Kafka and bronze; restart-safe | Restart and SIGKILL produced no duplicates; offsets resumed |
| 2 — Models and quality | Silver/gold, GX gate, scheduler, maintenance | GX failure blocked dbt; OPTIMIZE/VACUUM ran on schedule |
| 3 — Dashboard and 7 coins | Superset dashboard, multi-symbol ingestion | Dashboard reads live Athena; 7 symbols ingest |
| 4 — Docs and hardening | README, ADRs, runbook, cost, ticker stream, backfill | 30 ADRs + addendum; 118 tests; 128 dbt nodes |
| 5 — CI | Lint, types, offline tests on `main` pushes and pull requests | GitHub Actions run 40213500712 green |

The original build plan is now an [as-built plan](plan.md), not a to-do list.

## 9. Guarantees and evidence

| Guarantee | How it is enforced | Evidence |
|---|---|---|
| A replay does not duplicate trades | Bronze checks `(symbol, trade_id)` before append | 20k replay: 2 appended, 19,998 skipped |
| A rebalance does not duplicate trades | Refresh the Iceberg table before the scan; commit on revoke | 3→2→3 replica rebalance: 0 duplicates |
| Bad recent data does not reach gold | GX gate blocks dbt | Injected GX failure returned exit 1 and skipped dbt |
| Old duplicates are still caught | Whole-table integrity check every 72 cycles | Duplicate count exposed in `integrity_results` |
| A skipped run heals | dbt windows anchor on the target table | Late trades recompute the affected minute |
| The dashboard is reproducible | Superset provisioned through its REST API | `make superset` converges; 20 charts / 10 datasets |

These are tested claims, not adjectives. Known gaps are listed in the
[README](README.md#known-gaps) and [testing](docs/testing.md#known-gaps).

## 10. Cost and guardrails

Measured spend was **$3.40 for 16–24 October 2026**; the rough steady-state
projection is **$11–12/month** at current volume. S3 requests from many small files dominate; storage is cents.
Athena is $0.12 in the same window, spent on dbt builds and maintenance; GX and
integrity checks scan locally and add no Athena cost.

Controls:

- the $20 budget alert is declared first in the main pipeline stack (the tiny state
  bucket and lock table are created during bootstrap; OpenTofu may create
  independent resources concurrently);
- the pipeline IAM user has a scoped, least-privilege policy;
- live tables are partitioned by day and symbol, so queries scan less;
- writes are batched (45 seconds or 20,000 rows) to limit requests;
- `OPTIMIZE` and `VACUUM` run on a schedule;
- teardown is `tofu destroy` with human approval, after emptying the lake bucket;
  local volumes are removed separately.

Details and recommendations: [cost](docs/cost.md).

## 11. Definition of done

- [x] `make up` starts the local stack; OpenTofu provisions AWS.
- [x] Data flows from Binance to Superset without manual chart setup.
- [x] Quality checks and dbt tests pass in a scheduled run; failures block it.
- [x] Replays and rebalances do not create duplicates.
- [x] Archive backfill gives the dashboard at least 30 days of history.
- [x] README, architecture, data model, testing, ADRs, runbook, cost, and glossary exist.
- [x] Lint, types, and 118 offline tests pass; CI runs them on `main` pushes and
  pull requests.

## 12. Limits

- One Kafka broker is a single point of failure; a host sleep stops the pipeline.
- There is no trade archive. After a host pause, Kafka replays trades and quotes it
  already accepted, but events from the paused period are lost. Candles can be
  rebuilt from Binance's T+1 kline archive; quote history has no archive at all.
- Gold freshness follows the scheduler (~30–35 minutes), so this is near-real-time,
  not a streaming dashboard.
- Kafka replay is limited to the seven-day topic retention.
- CI does not run dbt, GX, or the live Kafka roundtrip; those need AWS or a broker.
- S3 expiration and tiering rules for Athena results and old bronze data are still
  pending; only abort-incomplete-upload is configured.
