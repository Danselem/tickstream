# Design decisions (ADRs)

Short records of each non-obvious choice: context, decision, consequence.

## ADR-001 — Bronze idempotency via upsert, not append-only

- Context: delivery is at-least-once; AGENTS.md says bronze is "raw, never modified"
  while Brief §9 asks for idempotent writes using MERGE on Iceberg.
- Decision: the consumer upserts each micro-batch on `(symbol, trade_id)` with
  PyIceberg's upsert API. "Raw" means no transformation/enrichment of trade facts; the
  upsert is a dedup-commit. Silver still re-dedupes as defense-in-depth.
- Consequence: replays never create duplicates; bronze uniqueness is enforced at write
  time. Guarded by a replay integration test (PyIceberg issue #3758).

## ADR-002 — Great Expectations validates a local PyIceberg scan

- Context: checks must gate every dbt build. Options: Athena SQL (realistic, costs per
  query) or a local scan of the new bronze window (free, same assertions).
- Decision: local scan via PyIceberg → pandas dataframe → GX checkpoint. Same suite
  semantics (schema, nulls, ranges, uniqueness, freshness), zero Athena spend.
- Consequence: Athena is exercised by dbt/Superset only; GX runs in seconds with no
  query cost. Trade-off recorded: a broken Athena path is not caught by GX.

## ADR-003 — OpenTofu state in S3 + DynamoDB, bootstrapped once

- Context: remote state with locking is the team-grade default; it needs a bucket and
  lock table that themselves need creating (chicken-and-egg).
- Decision: `infra/bootstrap/` (local state, two tiny resources) + `make
  infra-bootstrap` writes `infra/backend.hcl`. State encrypted at rest; lock file committed.
- Consequence: one extra one-time command; afterwards all state is remote and locked.

## ADR-004 — Single-broker KRaft Kafka, RF=1

- Context: portfolio MVP running on one machine; multi-broker adds nothing except cost
  and complexity at this throughput.
- Decision: official `apache/kafka` image, combined broker+controller, replication
  factor 1 everywhere (including `__consumer_offsets`).
- Consequence: single point of failure, accepted and documented. At scale: 3 brokers,
  RF=3, `min.insync.replicas=2`, idempotent + transactional producer.

## ADR-005 — Real AWS only; offline covered by pytest, not by compose
- Context: Brief says everything except S3/Glue/Athena runs locally, i.e. compose
  always talks to real AWS. An offline MinIO profile would add services and diverge.
- Decision: no MinIO/SQLite compose profile. Consumer catalog is config-driven
  (`CATALOG_TYPE=glue|sql`), so integration tests run fully offline against a SQL
  catalog + temp warehouse.
- Consequence: developing ingestion needs AWS credentials; tests don't.

## ADR-006 — Kafka topic: 3 partitions, symbol keys, 7-day retention

- Context: one symbol today, 6–7 tomorrow; single consumer; replays must be possible.
- Decision: `trades.raw` with 3 partitions (never shrinkable later), key = symbol
  (all XRP lands in one partition today; coins spread later), 7-day retention for
  replay headroom, producer-created at startup. Dual listeners: `kafka:19092`
  in-compose, `localhost:9092` on host.
- Consequence: verified 0 duplicates across restart + SIGKILL; lag metric initially
  over-reported on empty partitions (fixed in `metrics.py` + regression test).

## ADR-007 — One micro-batch commit = one Iceberg snapshot/file (for now)

- Context: 45 s batches currently yield ~1 small file per commit (14 files after
  ~10 min). Fine at this scale; becomes the small-files problem at 7 coins.
- Decision: accept for Phase 1; Phase 2 adds scheduled Athena `OPTIMIZE` + `VACUUM`.
- Consequence: nothing to do now except not letting it surprise us later.

## ADR-008 — Ops tables live in the `tickstream` Glue database

- Context: plan.md named them `ops.quality_results` / `ops.pipeline_metrics`, but
  infra provisions a single Glue database (`tickstream`, whose description already
  says "and ops tables"), and the pipeline IAM user lacks `glue:CreateDatabase`.
- Decision: `tickstream.quality_results` + `tickstream.pipeline_metrics`,
  unpartitioned append-only Iceberg tables created idempotently by
  `ensure_ops_tables()` (no infra change, no apply needed).
- Consequence: replaying a consumer batch can duplicate a metrics row (gap-free
  alternative chosen: metrics append happens *after* the offset commit, so a crash
  loses one row instead — bronze correctness unaffected either way).

## ADR-009 — dbt MERGEs carry an `update_condition` (immutable-event pattern)

- Context: trade facts never change, but the silver 2 h late-arrival lookback
  re-matches ~60k rows per run. Unconditional `WHEN MATCHED THEN UPDATE` rewrote
  them all → Glue `UpdateTable` timed out (`OperationTimeoutException`) twice.
- Decision: `update_condition` with null-safe `IS DISTINCT FROM` over all non-key
  columns on silver and gold (gold needs it for nullable `volatility`). Matched
  but identical rows are skipped; genuinely changed rows (e.g. late-arrival
  `ingest_time`, corrected candles) still update. GX uniqueness uses a derived
  `symbol:trade_id` column since trade_id is only unique per symbol.
- Consequence: incremental MERGEs commit in seconds; semantics unchanged
  (verified: second build merges new keys only, 0 dupes).

## ADR-010 — One IAM addition for dbt: `glue:GetTableVersions`

- Context: first `dbt build` failed — the dbt-athena adapter calls
  `GetTableVersions` when materializing relations, and the least-privilege
  allow-list didn't include it.
- Decision: added the single read-only action to `infra/iam.tf` (planned, reviewed,
  applied with approval; budget alert email preserved by re-planning with the real
  address — the default placeholder would have wiped it).
- Consequence: dbt builds green (33/33). Reminder: always re-plan infra with
  `ALERT_EMAIL=<real>` or the placeholder default silently replaces notifications.

## ADR-011 — Superset dashboard as code via a REST provision script

- Context: plan.md proposed committing an exported dashboard ZIP, but there is no
  browser in this loop to click one together, and hand-writing the export format
  (UUIDs, layout nodes) is fiddlier than the v1 REST API, which is stable and
  introspectable. `src/pipeline/superset/provision.py` (stdlib + urllib only,
  unit-tested with a fake transport) creates/updates the Athena database,
  datasets, 5 charts and the dashboard — idempotent, runs at container boot and
  via `make superset`.
- Decision: REST provisioning, not export-ZIP. Datasets point at gold + ops
  tables only, never bronze.
- Consequence: `make up` converges to a published dashboard with zero manual
  steps (Brief §11). Verified via SQL Lab through Superset: live row counts.

## ADR-012 — Superset-on-Athena gotchas (config, drivers, IAM, catalog case)
- Context: four independent failures, each with a misleading surface symptom:
  1. `SUPERSET_CONFIG` is a Python *module name*, not a file path, and the custom
     module *replaces* `superset.config` — so it must start with
     `from superset.config import *` (else `KeyError: 'DATA_DIR'`).
  2. The image runs from `/app/.venv` (pip-less, uv-built); system `pip`
     installs to a path outside `sys.path`. Driver install needs
     `python -m ensurepip` + venv `pip` (else "Connection failed" on every DB).
  3. Mutating API calls need BOTH the session cookie (cookie jar) and the
     `X-CSRFToken` header, even with a JWT bearer token.
  4. Athena metadata actions authorize against the **data catalog**, not the
     workgroup — `GetTableMetadata`/`ListDatabases` needed their own
     `datacatalog/AwsDataCatalog`-scoped statement (a workgroup-scoped grant is
     silently useless).
  5. PyAthena defaults `catalog_name` to lowercase `awsdatacatalog`, and Athena
     matches `datacatalog/<name>` case-sensitively — so reflection was denied
     despite a correct policy. The URI now pins `catalog_name=AwsDataCatalog`.
     (Diagnosed by calling the API with boto3 as the same principal: allowed —
     proving policy/principal fine and the difference was request params.)
- Decision: all five recorded here so the next database/driver/image doesn't
  re-pay the tuition. Prefer evidence over guessing: reproduce with the same
  principal, check CloudTrail, read the driver source in the image.
- Consequence: dataset sync + SQL Lab verified working; dashboard shows live data.

## ADR-013 — Bronze write path: pruned-scan merge, transient retry, S3 timeouts

- Context: Oct 2026 incident after the 7-coin launch. Three stacked failures:
  1. `table.upsert` does a Python-side full-table merge — O(table) per batch.
     At 300k rows a 20k batch ground 20+ min at 100% CPU with zero progress,
     while a ~1M backlog (4-day outage) plus fresh 7-coin inflow piled up.
  2. Transient S3 socket timeouts (pyarrow ~3 s defaults vs degraded Docker
     networking) crashed the consumer (5 restarts); each crash rebalanced the
     group and reset progress — a crash/rebalance livelock committing ~1 row
     per 45 s. Batches slower than `max.poll.interval.ms` (5 min) rebalanced
     even without crashing.
  3. `max.poll.interval.ms` default (5 min) is shorter than any retry storm.
- Decision:
  - `upsert_events` = scan only the batch's partition scope (day range ×
    symbols; manifest pruning), Arrow/Python anti-join on (symbol, trade_id),
    append only new rows, skip the commit entirely on full replays. Same
    guarantee (bronze duplicate-free, replay-safe), minutes → ~1 min/batch.
    Within-batch duplicate keys collapse to one row.
  - `common/retry.py` (tenacity): OSError (incl. pyarrow S3 timeouts),
    retryable botocore codes/5xx, PyIceberg CommitFailed; 8 attempts exp
    backoff. Bronze writes + table bootstrap retry; metrics append is
    best-effort (never breaks the data path).
  - `s3.connect-timeout=15 s` / `s3.request-timeout=60 s` via Settings (env),
    generous-but-bounded; harmless on healthy networks.
  - `max.poll.interval.ms=600000`: a single-consumer group never benefits from
    rebalance, and retry storms must fit inside the poll window.
- Consequence: first merged batch: 20k scanned, 2 appended, 19,998 skipped
  (stale backlog already in bronze — the overlap prediction held). Backlog
  drains ~20k/batch; fresh symbols land during the drain (partitions drain in
  parallel). Commit changed files + tests, not mechanism guarantees.

## ADR-014 — Consumer scale-out to 3 replicas + batching-window fix

- Context: after the 7-coin launch the single consumer could not keep up
  (~330–660 rows/s vs 7 streams), so the backlog grew. Diagnosis found three
  independent problems, only one of them capacity:
  1. **Batching-window bug (the real stall).** `TradeBatcher.drain()` reset the
     time window at the *start* of the flush. Once a flush (write + commit +
     metrics append) ran longer than `batch_interval_s` (45 s), the next window
     was already expired, so every following batch flushed the instant a single
     message arrived — a self-perpetuating 1-row batch every ~40 s, offsets
     advancing by 1. It looked like a network/lib stuck state but was pure
     control flow, and it reproduced on all three replicas.
  2. **librdkafka stuck state after churn.** 228 restarts (from the Oct 2026
     DNS/clock incident) left a consumer polling ~nothing until a clean
     restart. librdkafka itself was healthy (direct probe: 544 msgs/20 s).
  3. **Capacity.** One consumer over 3 partitions cannot match 7-coin ingest.
- Decision:
  - `drain()` no longer touches the timer; `start_window(now)` is called by the
    consumer **after** the write+commit, so a slow flush cannot leave the next
    window expired. Regression test in `test_batcher.py`.
  - Run **3 consumer replicas** (one per `trades.raw` partition) in the same
    `bronze-writer` group, via `--scale consumer=3` in the Makefile; the
    `container_name` was dropped so Compose can manage `tickstream-consumer-N`.
    Because the topic is keyed by symbol, each symbol owns exactly one partition
    and one replica — parallel writes never touch the same `(symbol, trade_id)`,
    and PyIceberg's optimistic-commit retry absorbs the metadata contention.
  - Raise the producer keepalive (`ws_ping_timeout_s=60`) to match Binance's
    20 s-ping / 1 min-pong contract (docs verified) and log `ws_throughput`.
- Consequence: offsets advance 20k/batch per partition again; lag drains
  ~1,300 rows/s with production ~25 rows/s. `max.poll.interval.ms=600000`
  (ADR-013) still holds; its "single-consumer group" rationale is superseded
  for scaling but kept for the poll-window bound. Caveat: a partition whose
  lag is much larger (partition 0 carried the historical XRP backlog) drains
  slower until the imbalance evens out — a rebalance after catch-up would
  re-spread the symbols.
