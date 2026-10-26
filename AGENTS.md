# AGENTS.md

## Project
Real-time crypto streaming pipeline (portfolio project). Ingests live Binance trades and 1 Hz top-of-book quotes, lands them in an Iceberg lake on AWS, models with dbt, validates with Great Expectations, visualises in Superset.

The project started with XRP only and now runs 7 USDT pairs (BTC, ETH, SOL, ADA, DOGE, LINK, AVAX). Keep the single-path discipline: prove a change on one symbol before expanding it.

## Architecture
Binance WebSocket -> producer -> Kafka -> consumer (PyIceberg) -> S3 Iceberg bronze (Glue catalog) -> Great Expectations -> dbt on Athena (silver, gold) -> Superset

A separate ticker producer/consumer path writes `ticker.raw` to `bronze_tickers` for top-of-book microstructure. Binance kline archives backfill chart history into `bronze_klines`.

Local (Docker Compose): producers, Kafka (KRaft, single broker), three trade consumers, ticker consumer, scheduler, Superset.
AWS: S3, Glue Data Catalog, Athena. Provisioned with OpenTofu.

## Stack (do not add or swap tools without asking)
Python 3.12 | uv | make | Docker Compose | Apache Kafka | PyIceberg | AWS S3 + Glue + Athena | dbt (dbt-athena) | Great Expectations | OpenTofu | Apache Superset

**Out of scope:** Spark, Airflow, equities, trading logic, ML.

## Repo layout
```
src/pipeline/{producer,consumer,common}/   Python code (typed, importable)
src/pipeline/{quality,scheduler}/          GX/integrity/health and the quality -> dbt loop
src/pipeline/{backfill,superset}/          Kline archive loader and dashboard provisioning
dbt/                                       models: staging, silver, gold
quality/                                   Great Expectations suites and reports
infra/                                     OpenTofu
tests/                                     pytest (unit + integration)
docs/                                      architecture, data model, testing, ADRs, runbook, cost, glossary
```

## Commands (the Makefile is the only entry point; add targets there)
`make install` | `make up` | `make down` | `make lint` | `make format` | `make typecheck` | `make test` | `make quality` | `make dbt-build` | `make infra-plan` | `make infra-apply`

Run `make lint typecheck test` before finishing any task.

## Tooling rules
- Python 3.12. Dependencies managed only with `uv` (`uv add`, `uv sync`, commit `uv.lock`). Never use pip directly.
- Lint and format with `ruff`; type-check with `mypy`; test with `pytest`.
- Full type hints on all functions. Small, single-purpose modules.

## Design rules
- **Delivery is at-least-once; processing must be idempotent.** Deduplicate on `(symbol, trade_id)`. Replays must never create duplicates.
- Commit Kafka offsets only after the Iceberg write succeeds.
- Write to Iceberg in micro-batches (30-60 s), never per event. Partition by date and symbol.
- Handle small files: compaction (Athena `OPTIMIZE`) and snapshot cleanup (`VACUUM`) are part of the design.
- Producer: reconnect with exponential backoff, handle ping/pong, shut down gracefully.
- All timestamps in UTC. Keep both event time and ingest time.
- Bronze = raw, never enriched. The write-time upsert/anti-join is a dedup commit, not a transformation; the only sanctioned mutation of `bronze_trades` is the documented duplicate repair (ADR-023), and `bronze_klines` is replaced only by the documented archive backfill (ADR-026). Silver = deduped and typed. Gold = 1-min OHLC, VWAP, volatility.
- Great Expectations must pass before each scheduled dbt build; a failure stops the run. Manual `make dbt-build` does not run GX, so run `make quality` first when the gate is needed.
- dbt: incremental models, tests on every model, documented columns.
- Separate configuration from code: env vars via `pydantic-settings`, with a committed `.env.example`.
- Structured logging (no `print`). Log lag, batch size, and freshness.
- Verify the Binance WebSocket endpoint against current Binance docs before use.

## Safety rules
- **Never commit secrets or credentials.** Use `.env` (gitignored) and least-privilege IAM.
- **Never run `make infra-apply` or `tofu destroy` without explicit human approval.** Plan first and show the output.
- The AWS budget alert is declared first in the main pipeline stack; the bootstrap state bucket and lock table are created before it. Declaration order does not guarantee parallel execution order. Never create costly resources (Redshift, large instances) unprompted.
- Do not delete data or reset Kafka offsets without asking.

## Workflow
- Work in phases: infra, XRP ingestion, dbt and quality, Superset, expansion, docs. Finish and verify one phase before starting the next.
- Small, focused commits with clear messages. Add or update tests with every change.
- If a requirement is unclear or a choice affects architecture, ask before building.
- Document each non-obvious design decision briefly in `docs/decisions.md`. Do not change the meaning of an accepted ADR: supersede or amend it with a new record. Clarity edits are allowed if the decision, evidence, heading text and anchor stay unchanged.
- Write for a reader who knows Python/SQL but not Kafka, Iceberg, dbt, or Superset. Put a plain-English summary at the top of each doc, define a term at first use or link `docs/glossary.md`, and keep technical detail below.
- Keep the README, architecture diagram, data model, testing evidence, runbook, cost figures and ADR index in sync when behavior, counts or cadence change.
