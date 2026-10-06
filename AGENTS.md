# AGENTS.md

## Project
Real-time crypto streaming pipeline (portfolio project). Ingests live Binance trades, lands them in an Iceberg lake on AWS, models with dbt, validates with Great Expectations, visualises in Superset.

**Start with XRP (XRPUSDT) only.** Expand to 6-7 coins after the single-coin pipeline works end to end.

## Architecture
Binance WebSocket -> producer -> Kafka -> consumer (PyIceberg) -> S3 Iceberg bronze (Glue catalog) -> Great Expectations -> dbt on Athena (silver, gold) -> Superset

Local (Docker Compose): producer, Kafka (KRaft, single broker), consumer, scheduler, Superset.
AWS: S3, Glue Data Catalog, Athena. Provisioned with OpenTofu.

## Stack (do not add or swap tools without asking)
Python 3.12 | uv | make | Docker Compose | Apache Kafka | PyIceberg | AWS S3 + Glue + Athena | dbt (dbt-athena) | Great Expectations | OpenTofu | Apache Superset

**Out of scope:** Spark, Airflow, equities, trading logic, ML.

## Repo layout
```
src/pipeline/{producer,consumer,common}/   Python code (typed, importable)
dbt/                                       models: staging, silver, gold
quality/                                   Great Expectations suites
infra/                                     OpenTofu
scheduler/                                 loop: quality checks, then dbt build
tests/                                     pytest (unit + integration)
docs/                                      architecture diagram, decisions
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
- Bronze = raw, never modified. Silver = deduped and typed. Gold = 1-min OHLC, VWAP, volatility.
- Great Expectations must pass before each dbt build; a failure stops the run.
- dbt: incremental models, tests on every model, documented columns.
- Separate configuration from code: env vars via `pydantic-settings`, with a committed `.env.example`.
- Structured logging (no `print`). Log lag, batch size, and freshness.
- Verify the Binance WebSocket endpoint against current Binance docs before use.

## Safety rules
- **Never commit secrets or credentials.** Use `.env` (gitignored) and least-privilege IAM.
- **Never run `make infra-apply` or `tofu destroy` without explicit human approval.** Plan first and show the output.
- The AWS budget alert is the first resource created. Never create costly resources (Redshift, large instances) unprompted.
- Do not delete data or reset Kafka offsets without asking.

## Workflow
- Work in phases: infra, XRP ingestion, dbt and quality, Superset, expansion, docs. Finish and verify one phase before starting the next.
- Small, focused commits with clear messages. Add or update tests with every change.
- If a requirement is unclear or a choice affects architecture, ask before building.
- Document each non-obvious design decision briefly in `docs/decisions.md`.
