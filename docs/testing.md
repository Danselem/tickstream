# Testing

**What this answers:** what the project proves, how to run each check, and where
the evidence stops.

There are four layers, each with a different job:

| Layer | What it protects | Needs |
|---|---|---|
| Offline unit tests | Python logic and control flow | Python + `uv` |
| dbt data tests | SQL models and table constraints | AWS + Athena |
| Great Expectations | Recent bronze data quality | AWS + Iceberg |
| Live acceptance checks | Kafka, rebalance, backfill, dashboard | The running stack |

Terms such as *dbt test*, *GX*, and *checkpoint* are defined in the
[glossary](glossary.md).

## Commands

| Command | What it runs | Success looks like |
|---|---|---|
| `make lint typecheck test` | Ruff, mypy, 118 offline unit tests | Exit 0; 118 passed, 1 deselected |
| `RUN_KAFKA_TESTS=1 uv run pytest -m integration` | One producer → Kafka → consumer roundtrip | Exit 0 against `localhost:9092` |
| `make quality` | GX suites over the live trade and quote windows | A report with all expectations green |
| `make integrity` | Whole-table duplicate check | `duplicates = 0` |
| `make dbt-build` | 128 dbt nodes (14 models + 114 data tests) | All models and tests pass; this target does **not** run GX |
| `make health` | Row counts and per-symbol freshness | Fresh ops rows in the health tables |

`make test` excludes the integration marker, so the default suite is offline and
safe in CI. The live roundtrip uses a unique topic and consumer group, and skips
unless `RUN_KAFKA_TESTS=1`.

Run one test while debugging:

```bash
uv run pytest tests/unit/test_batcher.py -q
uv run pytest tests/unit/test_bronze_writer.py::test_replay_does_not_duplicate_rows -q
```

## Unit suite

The unit tests do not need AWS, Kafka, or Docker. They cover the parts where a
small logic error is expensive:

| Test file | What it proves |
|---|---|
| `test_models`, `test_config` | Binance payload validation, UTC normalisation, env-driven settings |
| `test_producer_ws` | Ping/pong, reconnect, and the per-message silence watchdog |
| `test_batcher` | Size/interval flushes and that the next window starts after the flush |
| `test_bronze_writer` | Partition-scoped anti-join, replay adds nothing, key handling |
| `test_kafka_record` | Explicit symbol→partition routing and invalid-partition rejection |
| `test_retry` | Transient S3/Kafka/commit failures retry; permanent failures fail fast |
| `test_scheduler` | GX and integrity failures block dbt; health runs before the gate; maintenance ordering |
| `test_gx_runner` | Empty, stale, duplicate, and non-positive-price windows fail; overlap is correct |
| `test_integrity`, `test_health` | Duplicate counts, layer counts, freshness, ops-table writes |
| `test_ops_tables`, `test_metrics` | Ops schemas, Arrow round-trips, lag edge cases |
| `test_dbt_runner`, `test_maintenance` | dbt invocation, OPTIMIZE/VACUUM ordering and failure isolation |
| `test_repair`, `test_klines_backfill` | Dedup repair, archive parsing, file selection |
| `test_superset_provision` | Chart/dataset specs, filters, layout, labels, idempotent updates |

The tests use fakes and temporary catalog state. They prove control flow, not that
AWS or a live broker is configured correctly.

## Data and live checks

### Great Expectations

GX reads from the last successful run, minus a five-minute overlap, so the newest few minutes are checked twice. It checks:

- required columns exist;
- key fields are not null;
- price and quantity are positive;
- `(symbol, trade_id)` is unique in the window;
- at least one row arrived;
- the newest event is no older than 300 seconds.

Manual runs write reports to `quality/reports/`; scheduled runs write inside the
scheduler container and summarize in `tickstream.quality_results`. A failure exits
non-zero, so the scheduler skips dbt and does not advance the window.

### Whole-table integrity

The integrity check scans only `symbol` and `trade_id` across all bronze rows:

```text
duplicates = rows − distinct_keys
```

It runs every 72 scheduler cycles. Any duplicate blocks dbt. This is the backstop
for duplicates outside the recent GX window
([ADR-016](decisions.md#adr-016--whole-table-integrity-check-on-the-maintenance-cadence)).

### dbt tests

The scheduled run always executes GX first. A manual `make dbt-build` skips GX,
so run `make quality && make dbt-build` when you need the gate.

Every model has data tests: unique keys, not-null fields, accepted values, OHLC
bounds, and positive volume/notional. `dbt build` runs the tests with the models,
so a failed test stops the build before the table is treated as ready.

### Live acceptance evidence

| Check | Result |
|---|---|
| Restart and SIGKILL mid-stream | 0 duplicate rows; offsets resumed |
| 3 → 2 → 3 consumer rebalance | 0 duplicates |
| 20,000-row stale replay | 2 appended, 19,998 skipped |
| Injected GX failure | Exit 1; dbt skipped |
| Archive backfill | 846,720 checksum-verified rows |
| Ticker stream | ~5–7 messages/s; full 45 s batches, not one-row batches |
| Dashboard | 20 charts / 10 datasets read live Athena rows |

These checks need the running stack or AWS and are run manually. CI cannot prove
them.

## CI

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs on every push to
`main` and every pull request:

1. install dependencies with `uv`;
2. fail if `uv.lock` changed during install;
3. run `make lint typecheck test`.

It does **not** run dbt, GX, integrity, or the Kafka integration test. Those need
AWS or a live broker and are verified manually. The CI badge means “lint, types,
and offline unit tests pass”, not “the pipeline is live”.

## What is green, and what it does not mean

| Green check | It means | It does not mean |
|---|---|---|
| `make lint typecheck test` | Code style, types, and unit logic are sound | The cloud path works |
| `make quality` | The recent bronze window passes the rules | All history is clean |
| `make integrity` | No duplicate trade keys exist now | Future writes cannot duplicate |
| `make dbt-build` | Models and their data tests pass | Superset is configured |
| `make health` | Rows landed and freshness was measured | Every chart is correct |

## Known gaps

- The caller ordering inside `_flush()` is verified live, not by a unit test. The
  batcher tests cover window semantics only.
- dbt, GX, and the Kafka roundtrip are opt-in and not part of CI.
- There is no cross-version Iceberg compatibility suite. Replay safety is covered
  at the table level with the SQL catalog.
- The whole-table integrity check is periodic, so a duplicate can survive until
  the next maintenance cycle.
- Superset chart rendering is checked by provisioning tests and manual review, not
  by browser tests.
