# Testing

Three layers, each with a different job: offline pytest for code and control
flow, dbt tests for modelled data, and live checks for infrastructure the unit
suite cannot prove.

## Commands

| Command | What runs | Needs |
|---|---|---|
| `make lint typecheck test` | Ruff, mypy, 118 offline unit tests | Python + `uv` |
| `RUN_KAFKA_TESTS=1 uv run pytest -m integration` | One live producer→Kafka→consumer roundtrip | Running Kafka at `localhost:9092` |
| `make quality` | GX suites over the live bronze window | AWS + Iceberg |
| `make integrity` | Whole-table duplicate check | AWS + Iceberg |
| `make dbt-build` | 128 dbt nodes (14 models + 114 data tests) | AWS + Athena |
| `make health` | Row counts and per-symbol freshness | AWS + Iceberg |

`make test` excludes the integration marker, so the default suite is offline and
safe in CI. The live integration test uses a unique topic and consumer group, and
skips unless `RUN_KAFKA_TESTS=1`.

## Unit suite

| Area | Proves |
|---|---|
| `test_models`, `test_config` | Binance payload validation, UTC normalisation, env-driven settings |
| `test_producer_ws` | Ping/pong, reconnect, and the per-message silence watchdog |
| `test_batcher` | Size/interval flushes and that the window resets after the flush |
| `test_bronze_writer` | Partition-scoped anti-join, replay adds nothing, key handling |
| `test_kafka_record` | Explicit symbol→partition routing and invalid-partition rejection |
| `test_retry` | Transient S3/Kafka/commit failures retry; permanent failures fail fast |
| `test_scheduler` | GX and integrity failures block dbt; health runs before the gate; maintenance ordering |
| `test_gx_runner` | Empty, stale, duplicate, and non-positive-price windows fail; window overlap is correct |
| `test_integrity`, `test_health` | Duplicate counts, layer counts, freshness, ops-table writes |
| `test_ops_tables`, `test_metrics` | Ops schemas, Arrow round-trips, lag edge cases |
| `test_dbt_runner`, `test_maintenance` | dbt deps/build invocation, OPTIMIZE/VACUUM ordering and failure isolation |
| `test_repair`, `test_klines_backfill` | Dedup repair, archive parsing, monthly/daily file selection, checksum paths |
| `test_superset_provision` | Chart/dataset specs, filters, layout, labels, idempotent database updates |

## Data and live checks

- **GX** validates the last successful window minus a 5 minute overlap: required
  columns, key nulls, price/quantity ranges, key uniqueness, row count, and
  freshness ≤ 300 s. Reports live in `quality/reports/` and
  `tickstream.quality_results`.
- **Integrity** scans only key columns over the whole bronze table:
  `duplicates = rows − distinct_keys`. It runs every 72 scheduler cycles and any
  duplicate blocks dbt ([ADR-016](decisions.md#adr-016--whole-table-integrity-check-on-the-maintenance-cadence)).
- **dbt tests** cover uniqueness, not-null, accepted values, OHLC bounds, and
  volume/notional signs. They run as part of `dbt build`, so a failing test stops
  the model build.
- **Live acceptance evidence:** restart/SIGKILL mid-stream and a 3→2→3
  rebalance produced 0 duplicates; a 20k-row stale replay appended 2 and skipped
  19,998; GX failure injection blocked dbt with exit 1; the archive backfill
  loaded 846,720 checksum-verified rows.

## CI

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs on every push to
`main` and every pull request:

1. install dependencies with `uv`;
2. fail if `uv.lock` changed during install;
3. run `make lint typecheck test`.

It does **not** run dbt, GX, integrity, or the Kafka integration test: those need
AWS or a live broker and are verified manually. The CI badge therefore means
"lint, types, and offline unit tests pass", not "the pipeline is live".

## Known gaps

- The caller ordering in `_flush()` is verified live, not by a unit test; the
  batcher tests cover the window semantics only.
- dbt, GX, and the Kafka roundtrip are opt-in and not part of CI.
- There is no cross-version Iceberg compatibility suite; replay safety is
  covered at the table level with the SQL catalog.
