# Design decisions (ADRs)

Short records of each non-obvious choice: context, decision, consequence.
Accepted records are not rewritten; when a decision changes, a new ADR supersedes
or amends the old one and the status below is updated. `ADR-012a` is an addendum
kept at its historical position.

| ADR | Decision | Status |
|---|---|---|
| [ADR-001](#adr-001--bronze-idempotency-via-upsert-not-append-only) | Bronze idempotency via upsert, not append-only | Superseded on the write path by ADR-013 (key unchanged) |
| [ADR-002](#adr-002--great-expectations-validates-a-local-pyiceberg-scan) | GX validates a local PyIceberg scan | Accepted |
| [ADR-003](#adr-003--opentofu-state-in-s3--dynamodb-bootstrapped-once) | OpenTofu state in S3 + DynamoDB | Accepted |
| [ADR-004](#adr-004--single-broker-kraft-kafka-rf1) | Single-broker KRaft Kafka, RF=1 | Accepted |
| [ADR-005](#adr-005--real-aws-only-offline-covered-by-pytest-not-by-compose) | Real AWS only; offline covered by pytest | Accepted |
| [ADR-006](#adr-006--kafka-topic-3-partitions-symbol-keys-7-day-retention) | Kafka topic: 3 partitions, symbol keys, 7-day retention | Amended by ADR-014 (consumer count) |
| [ADR-007](#adr-007--one-micro-batch-commit--one-iceberg-snapshotfile-for-now) | One micro-batch = one Iceberg snapshot/file | Accepted (implemented by scheduled OPTIMIZE/VACUUM) |
| [ADR-008](#adr-008--ops-tables-live-in-the-tickstream-glue-database) | Ops tables in the `tickstream` Glue database | Accepted |
| [ADR-009](#adr-009--dbt-merges-carry-an-update_condition-immutable-event-pattern) | dbt MERGEs carry an `update_condition` | Accepted |
| [ADR-010](#adr-010--one-iam-addition-for-dbt-gluegettableversions) | One IAM addition for dbt: `glue:GetTableVersions` | Accepted |
| [ADR-011](#adr-011--superset-dashboard-as-code-via-a-rest-provision-script) | Dashboard as code via REST provisioning | Accepted |
| [ADR-012](#adr-012--superset-on-athena-gotchas-config-drivers-iam-catalog-case) | Superset-on-Athena gotchas | Accepted |
| [ADR-012a](#adr-012a--dashboardchart-linkage-on-superset-60-and-the--in-chart-names) | Dashboard↔chart linkage on Superset 6.0 | Accepted |
| [ADR-013](#adr-013--bronze-write-path-pruned-scan-merge-transient-retry-s3-timeouts) | Bronze write path: pruned-scan merge, retry, timeouts | Accepted |
| [ADR-014](#adr-014--consumer-scale-out-to-3-replicas--batching-window-fix) | Consumer scale-out to 3 replicas + batching window | Amended by ADR-030 (metrics ordering) |
| [ADR-015](#adr-015--cross-replica-duplication-on-rebalance-refresh-before-scan--revoke-commit) | Cross-replica duplication on rebalance | Accepted |
| [ADR-016](#adr-016--whole-table-integrity-check-on-the-maintenance-cadence) | Whole-table integrity check | Accepted |
| [ADR-017](#adr-017--business-gold-summary--tabbed-dashboard-two-audiences) | Business gold summary + tabbed dashboard | Accepted |
| [ADR-018](#adr-018--dashboard-readability-explicit-labels-legends-and-stable-tab-ids) | Dashboard readability | Accepted |
| [ADR-019](#adr-019--dimension-views-dim_symbol-dim_time) | Dimension views | Accepted |
| [ADR-020](#adr-020--market-overview--native-dashboard-filters) | Market overview + native filters | Accepted |
| [ADR-021](#adr-021--lake-level-health-per-layer-row-counts-and-per-symbol-freshness) | Lake-level health | Accepted |
| [ADR-022](#adr-022--trailing-time-windows-for-provisioned-charts-now-nd--now) | Trailing time windows | Accepted |
| [ADR-023](#adr-023--repair-bronze-duplicates-server-side-delete-both--insert-one) | Repair bronze duplicates server-side | Accepted |
| [ADR-024](#adr-024--incremental-windows-anchor-on-the-target-with-overridable-lookbacks) | Target-anchored incremental windows | Accepted |
| [ADR-025](#adr-025--treat-a-silent-websocket-as-dead-message-watchdog) | Treat a silent WebSocket as dead | Accepted |
| [ADR-026](#adr-026--archive-1m-klines-backfill-for-chart-history-bronze_klines) | Archive 1m klines backfill | Accepted |
| [ADR-027](#adr-027--readable-default-view-btc-24-h-detail--30-d-trend) | Readable default view | Accepted |
| [ADR-028](#adr-028--explicit-symbolpartition-routing-for-tradesraw) | Explicit symbol→partition routing | Accepted |
| [ADR-029](#adr-029--separate-ticker-stream-for-top-of-book-microstructure) | Separate `@ticker` stream | Accepted |
| [ADR-030](#adr-030--start-the-batch-window-after-the-whole-flush) | Start the batch window after the whole flush | Accepted |

## ADR-001 — Bronze idempotency via upsert, not append-only

- Status: superseded on the write path by ADR-013; the merge key
  `(symbol, trade_id)` and the "raw means no enrichment" definition still hold.
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

- Status: accepted; the "single consumer" assumption is superseded by ADR-014,
  while the topic size, key and retention are unchanged.
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
  datasets, an initial 5 charts and the dashboard — idempotent, runs at container boot and
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

## ADR-012a — Dashboard↔chart linkage on Superset 6.0 (and the `+` in chart names)
- Context: the dashboard rendered every tile as "There is no chart definition
  associated with this component, could it have been deleted?". Root cause: in
  Superset **6.0.0** the dashboard **write** schemas have no `charts` field (it
  is read-only), and the `dashboard_slices` association is populated *only* by
  `DashboardDAO.set_dash_metadata`, which `UpdateDashboardCommand` runs when
  `json_metadata` is present and reads `positions` from it. `CreateDashboardCommand`
  never syncs slices. Our provision script sent `position_json` but no
  `json_metadata`, so `dashboard_slices` stayed empty and the frontend could not
  resolve any chart. Separately, `ensure_chart` matched by name through the list
  API's `slice_name eq` filter, which returns **no rows** for names containing
  `+` ("Consumer lag + batch rows"), so that chart was re-created on every run
  (11 duplicates) while every other name stayed single.
- Decision: send `json_metadata = {"positions": layout}` on the dashboard PUT
  (and always issue a PUT after create so slices link); match charts by listing
  and comparing `slice_name` in Python (mirrors the existing dataset helper)
  rather than trusting the server-side name filter.
- Consequence: `dashboard_slices` is populated, all tiles render, and provisioning
  is truly idempotent (chart count stable across runs). Verified by reading the
  metadata DB directly and by running `make superset` twice.


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

- Status: accepted; amended by ADR-030 (the window must start after metrics too).
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

## ADR-015 — Cross-replica duplication on rebalance; refresh-before-scan + revoke commit

- Context: after scaling to 3 replicas (ADR-014) and restarting the group, bronze
  gained **15,825 duplicate `(symbol, trade_id)` keys** — otherwise identical
  rows — clustered at the restart. `total − distinct ≈ 15,825`, `max rows per
  key = 2`. The GX bronze suite (`expect_column_values_to_be_unique` on
  `unique_key`) would therefore fail and block dbt.
- Root cause (reproduced offline with two `Table` handles on the SQL catalog):
  each consumer process caches its own `Table` object and only sees **its own**
  appends reflected in that object. On a rebalance the new partition owner
  replays a batch the old owner already wrote, but its cached metadata predates
  that write, so the partition-scoped anti-join (ADR-013) finds nothing and
  **appends the rows again**. Kafka's at-least-once delivery plus a stale local
  snapshot — not a merge bug — produced the duplicates.
- Decision:
  - `upsert_events` calls `table.refresh()` **before** the anti-join scan so a
    replica sees every replica's committed rows. Regression test
    `test_cross_replica_replay_does_not_duplicate_rows` (two writers, one stale).
  - Harden the trigger: `partition.assignment.strategy=cooperative-sticky` and an
    `on_revoke` handler that flushes + commits the in-flight window before the
    new owner takes the partitions, so replays are rare and small.
  - Repair the existing damage once with `make dedup-bronze`, run with the
    consumers stopped. Crafted as an in-process PyIceberg rewrite
    (`scan -> keep earliest row per key -> table.overwrite`), **not** Athena SQL:
    Athena engine v3 is Trino-based and has no `INSERT OVERWRITE` (only
    `INSERT INTO` / `MERGE` / `DELETE`), and neither MERGE nor DELETE can remove
    one of two *identical* rows. This is the single sanctioned exception to
    "bronze is raw and never modified"; it restores the no-duplicate invariant
    the GX gate depends on.
- Consequence: the anti-join is now correct across replicas as long as no two
  consumers write the same key in the same instant. That last window is closed
  by single-owner-per-partition + commit-on-revoke; a truly concurrent
  double-write would still require Iceberg row-level MERGE, which we avoid
  because PyIceberg's partitioned upsert is buggy (#3758). Residual duplicates
  are caught by the GX uniqueness gate rather than silently accumulating.

## ADR-016 — Whole-table integrity check on the maintenance cadence

- Context: ADR-015 left the windowed GX uniqueness gate (ADR-002) as the last
  line of defense. That gate only validates the recent window, so a duplicate
  introduced *outside* it (e.g. a stale handle replaying an old batch) could
  hide until the window happened to cover it. The 15.8k-row incident was caught
  only by a manual full-table query, not by the scheduled run.
- Decision: add a coarse, low-frequency whole-table check, separate from the
  frequent windowed GX run.
  - `quality/integrity.py` scans **only** the key columns (`symbol`, `trade_id`)
    of the whole bronze table via local PyIceberg (zero Athena cost), computes
    `duplicates = total_rows - distinct_keys`, and appends one row to a new
    `tickstream.integrity_results` ops table (ADR-008 conventions).
  - The scheduler runs it on the **maintenance cadence** (`MAINTENANCE_EVERY_N_RUNS=72`,
    ≈1.5–2 days at the current ~34 min cycle), *before* dbt; `duplicates > 0` fails
    the cycle and blocks dbt (Brief §6), matching the "a failure stops the run" rule.
  - Exposed ad hoc as `make integrity` (`--integrity-only`).
- Consequence: keeps the frequent/cheap windowed gate AND a whole-table safety
  net, with no extra Athena spend (~19 s to scan 4.3M key rows). Trade-off: the
  whole-table scan is O(table) in Python, so it is deliberately NOT run every
  cycle; a duplicate could persist up to one maintenance interval before the
  run blocks. `integrity_results` also plots duplicate-rate over time in Superset.

## ADR-017 — Business gold summary + tabbed dashboard (two audiences)

- Context: the dashboard was an engineering/operations view (lag, quality,
  integrity) plus raw market candles, with no business metrics. A manager needs
  USD notional, price range, % change and trades/min; an on-call engineer needs
  the ingestion/quality signals. Mixing both on one grid served neither.
- Decision: put the business math in the **gold layer** and split the dashboard.
  - `gold_candles_1m` gains `notional = sum(price*quantity)` (= `vwap × volume`).
  - New `gold_market_summary_daily` / `_hourly` (one shared macro
    `market_summary(grain, lookback)`): per `(symbol, window_start)` open/close,
    high/low, notional, volume, trade_count, trades_per_min,
    `change_pct = (close-open)/open`. Incremental MERGE with a trailing
    recompute (2 days / 2 hours) so late trades self-correct.
  - Two native Superset tabs on one dashboard: **Market** (business) and
    **Pipeline health** (ops), built as `ROOT → TABS → TAB → ROW → CHART`.
- Consequence: each audience gets a clean view; aggregation logic lives in dbt
  (tested, one source of truth), Superset stays declarative. `%change` cannot be
  a Superset SIMPLE metric (no first/last aggregate), which is why a summary
  model — not a chart-layer calc — is the implementation. Adding `notional` to
  an existing Iceberg table needs a one-off `make dbt-refresh` (no
  `on_schema_change`), and the two summary tables join the OPTIMIZE/VACUUM list.

## ADR-018 — Dashboard readability: explicit labels, legends, and stable tab ids

- Context: the provisioned charts rendered as unlabelled walls of lines — axis
  titles, legends and time formatting were all left to control-panel defaults,
  tables showed raw column names (`window_start`, `change_pct`), and the ops
  chart overlaid lag (messages) and batch rows (count) on one axis. A chart with
  no labels answers no business question. Tab ids were index-based
  (`TAB-0`/`TAB-1`), which would silently detach a native filter if a tab moved.
- Decision: make readability explicit in the provisioning code.
  - `timeseries_params` always sets `x_axis_title`/`y_axis_title`,
    `show_legend`+`legendType=scroll`+`legendOrientation=top`,
    `x_axis_time_format`, `rich_tooltip`+`tooltipTimeFormat`, and
    `color_scheme`; each `ChartSpec` carries its own axis titles.
  - A single `COLUMN_LABELS` map feeds `Api.label_columns`, which re-sends the
    **full** column list (each with its existing `id`, `type`, `is_dttm`) as
    `verbose_name`; a partial list would drop columns, and omitting `id` fails
    with "columns already exist".
  - Tables get `table_timestamp_format` + `show_cell_bars`; the ops "lag + rows"
    chart is split into two charts (different units do not belong on one axis).
  - `dashboard_layout` derives tab ids from the title (`TAB-Market`,
    `TAB-Pipeline-health`) via `tab_key`, so `tabsInScope` stays stable.
- Consequence: charts are self-describing and safe to reorder; the label/legend
  settings are code (unit-tested) rather than per-chart clicks in the UI, so a
  rebuilt dashboard is always readable. Trade-off: labels live in a map that
  must be extended when new columns are surfaced.

## ADR-019 — Dimension views (`dim_symbol`, `dim_time`)

- Context: the gold layer is fact-shaped (`gold_candles_1m`, summaries) with no
  dimensions. A BI layer benefits from a symbol dimension (base/quote) and a time
  spine with calendar attributes (for later hour/weekday heatmaps), but renaming
  the existing facts to `fct_*` would churn every model, test and doc for little
  payoff.
- Decision: add two **views** and keep the existing fact names (assessment of the
  external "model it dimensionally" advice).
  - `dim_symbol`: one row per pair (base/quote parsed, first/last seen) —
    `group by symbol` over silver.
  - `dim_time`: a bounded minute spine from the candles' time range via Trino
    `sequence` + `unnest`, with hour/day/weekday/hour_of_day.
- Consequence: a clean, cheap dimension layer without a rename. Both are views,
  so `dim_time` is regenerated per query (bounded by the observed range) and has
  no storage cost; it is not yet wired to a chart (it serves the future risk
  dashboard). Trade-off: a view re-computes the spine each query rather than
  materialising it.

## ADR-020 — Market overview + native dashboard filters

- Context: the Market tab showed only time-series, no at-a-glance summary, and
  without filters every chart plotted all 7 coins at once. Viewers needed a
  candlestick price view, proportional volume, headline figures and the ability
  to focus a symbol.
- Decision: extend the Market tab and add dashboard-level native filters.
  - New charts: native **candlestick** (`viz_type=candlestick`; metric keys
    `open`/`close`/`high`/`low`, dimension key `series`, verified against the
    Superset 6 control panel), **treemap_v2** of notional share, and two
    **big_number_total** KPI tiles (notional, trades). Superset 6 keys are
    version-specific: `treemap_v2`/`heatmap_v2`/`histogram_v2`, `big_number`,
    `big_number_total`, `candlestick` (not the old deck.gl `treemap`).
  - Native filters live in `dashboard.json_metadata.native_filter_configuration`:
    a **Symbol** `filter_select` (targets the gold dataset by **uuid**, scoped to
    the gold candles on the Market tab) and a global **Time range** `filter_time`.
    `ensure_dashboard` sends `positions` and the filters in **one** `json_metadata`
    (the PUT replaces it wholesale).
- Consequence: the overview answers "what is happening now" at a glance and can
  be focused per coin, while summary tables keep all coins. Trade-off: candlestick
  correctness assumes the 1-minute grain (one row per candle, so `MAX(open)` is
  the open); changing the chart's time grain would need first/last aggregation.
  `uuid` is required for filter targets, so provisioning fails loudly if the
  dataset has none.

## ADR-021 — Lake-level health: per-layer row counts and per-symbol freshness

- Context: the Pipeline health tab showed Kafka lag, batch rows, quality runs and
  bronze uniqueness — all *pipeline* signals — but nothing about the lake itself:
  did rows actually land in bronze/silver/gold, and how stale is each symbol? The
  quality gate only looks at a recent window; a stuck layer would only show up
  indirectly, later.
- Decision: compute two lake-health facts once per maintenance cycle and surface
  them on the health tab.
  - `pipeline.quality.health.compute_health` (pure) takes the loaded layer tables
    plus `now`, and returns `LayerCount` rows (`table_row_count` per layer) and
    `SymbolFreshness` rows (`now − max(event_time)` per symbol from gold).
  - `run_pipeline_health` loads bronze/silver/gold, skips any missing table
    (`NoSuchTableError`), and appends to two new Glue ops tables — `layer_counts`
    (`layer`, `row_count`) and `symbol_freshness` (`symbol`, `max_event_time`,
    `freshness_seconds`) — via the same append+arrow path as the other ops tables.
  - `table_row_count` prefers the snapshot summary `total-records` (O(1)); it falls
    back to a single-column (`symbol`) scan, which is what pyiceberg's append
    currently requires for large tables.
  - The scheduler runs it inside `with_maintenance` (after the integrity check,
    before dbt) and exposes `--health-only` / `make health` for an ad-hoc refresh.
  - Two Health charts read the tables: "Row counts by layer (bronze → gold)" and
    "Data freshness by symbol (seconds)".
- Consequence: the health tab now answers "is data landing and is it fresh", and
  the tables are queryable in Athena for alerting later. Trade-off: counts are a
  maintenance-cadence snapshot, not continuous; on the current data volume the
  fallback scan costs ~50 s for a full refresh (acceptable off the hot path).

## ADR-022 — Trailing time windows for provisioned charts (`now-Nd : now`)

- Context: every chart was provisioned with the Superset preset `time_range="Last
  day"`. Superset's named presets (`Last day`, `Last 7 days`, `today`) are anchored
  to **midnight** and exclude the current day: "Last day" renders `>= yesterday
  00:00 AND < today 00:00`. The freshly-collected ops rows (written today) therefore
  rendered as *no data*, and the "today" KPIs were silently showing *yesterday*.
- Decision: use explicit trailing-window ranges relative to now instead of presets.
  - Constant `TIME_RANGE = "now-7d : now"` for series/ops charts; `SUMMARY_TIME_RANGE
    = "now-1d : now"` for the daily/hourly summary tables, the notional treemap and
    the two KPI tiles. `ChartSpec` carries a per-chart `time_range`, threaded into
    every param builder (they already passed it to `big_number_params`).
  - Rename labels that implied a calendar day ("today") to name the window
    ("last 24h"), so the title matches the filter.
- Consequence: the newest point is always in view and titles match the data window.
  Trade-off: `now-…` ranges are evaluated per request (the SQL is not deterministic
  across days), which is fine for a live dashboard but means screenshots/exported
  SQL differ day to day.

## ADR-023 — Repair bronze duplicates server-side (`DELETE` both + `INSERT` one)

- Context: one duplicate key (`BTCUSDT:6774376484`, two identical rows) in 1.7 M
  rows made the GX uniqueness gate fail forever, because the gate validates the
  whole window and the window only advances on success. The local PyIceberg
  rewrite (`scan -> dedup -> overwrite`) was too slow over S3 on the current
  uncompacted layout: a 3-day rewrite ran >15 min without committing, and even a
  key-filtered local scan stayed too slow.
- Decision: repair the key in Athena (server-side, prunes by partition and file
  stats): `DELETE ... WHERE symbol=... AND trade_id=...` removes both identical
  rows, then `INSERT` the one row back. That is safe because the key is unique
  after the delete and the row value is deterministic. The PyIceberg path stays
  for small/compacted tables and offline tests (`dedup_key`/`make dedup-key`).
- Consequence: the Oct 2026 repair committed in ~40 s (2 -> 0 -> 1 row) and the
  gate passed (1.85 M rows). Trade-off: it is a manual, two-statement procedure
  (consumers stopped), documented in `docs/runbook.md`; the durable fix is
  compaction, which the maintenance loop already runs.

## ADR-024 — Incremental windows anchor on the target, with overridable lookbacks

- Context: gold's incremental filter started at `max(event_time)` of
  **`silver_trades`** (the source), not of the gold table. After a skipped run the
  source is already current, so the next build only recomputed the last 30
  minutes and left a permanent hole. Silver/gold also had holes from dbt being
  skipped whenever the quality gate failed; the fixed 2 h/30 min lookbacks cannot
  repair them.
- Decision: anchor both windows on `{{ this }}` (`max(event_time)` for silver,
  `max(candle_start)` for gold) and make the lookbacks dbt vars —
  `silver_lookback_hours` (default 2) and `gold_lookback_minutes` (default 30).
  A one-off run with large values repairs a gap, e.g.
  `dbt run --select silver_trades --vars '{silver_lookback_hours: 120}'` then the
  same for `gold_candles_1m` with `gold_lookback_minutes: 7200`.
- Consequence: a skipped run is caught up on the next success, and an operator can
  heal a long outage without a full refresh. Trade-off: a stale table now drives a
  larger scan on recovery (still bounded and idempotent via the MERGE keys).

## ADR-025 — Treat a silent WebSocket as dead (message watchdog)

- Context: the producer's keepalive only detects a dead TCP path. In Oct 2026 the
  combined stream stayed open and kept answering pings while delivering **no
  trades for hours**, so bronze lost whole hours (the producer logged throughput
  only when a message arrived, hiding the silence). A ping timeout alone
  (`ws_ping_timeout_s=60`) never fired.
- Decision: read with `asyncio.wait_for(ws.recv(), timeout=ws_stale_after_s)`
  (default 60 s). On timeout raise `StaleStreamError`, log `ws_stalled`, and
  reconnect through the existing backoff. Keep the keepalive for dead TCP.
- Consequence: a silent subscription recovers within ~1 min instead of hours.
  Trade-off: 60 s of global silence is abnormal for 7 symbols (BTC/ETH trade
  every second); if a very quiet symbol set is ever configured, raise the
  threshold. Historical holes are repaired by the archive backfill, not by this.

## ADR-026 — Archive 1m klines backfill for chart history (`bronze_klines`)

- Context: the live pipeline only held ~2 days of usable candles (sparse, because
  of the silent-WS gaps in ADR-025), so the 7 d/30 d charts were mostly empty and
  a 1-minute series could not show a month of context. Binance publishes spot
  `klines` archives (monthly for complete months, daily for the current month);
  there is **no** historical depth/order-book archive, so klines are the only
  cheap way to rebuild OHLCV.
- Decision: a separate, offline backfill instead of replaying trades.
  - `src/pipeline/backfill` downloads `data.binance.vision` zips for the
    configured symbols, verifies each `.CHECKSUM` (SHA256), and turns the
    headerless CSV into an Arrow/Iceberg table `bronze_klines` (symbol-partitioned;
    one row per `(symbol, open_time)`, `source='archive'`). Spot archive
    timestamps are **microseconds** since 2025-01 (13-digit ms handled too); they
    are normalised to naive UTC, matching the live bronze contract.
  - `make backfill` defaults to the first day of the month 60 days ago through
    yesterday, all `TICKSTREAM_SYMBOLS`; `FORCE=1` replaces the table in one
    commit (a normal re-run skips when rows exist, so a stray run cannot
    duplicate history).
  - dbt: `stg_klines` → `silver_klines` (view + `row_number` dedup) →
    `gold_candles_1m_hist` (`volatility` is NULL; `vwap = quote_volume/volume`)
    → `gold_candles_1m_unified` view: live candles win per minute, archive fills
    the rest, with a `source` column (`live`/`kline`).
  - The offline load is gated by the SHA256 sidecars plus dbt tests (unique
    `(symbol, open_time)`, non-null/positive OHLC, `high >= low`, volume ≥ 0),
    not by the live GX window (which stays scoped to the last successful gate).
- Consequence: the first load covered 806,400 candles (Aug 1–Oct 19 2026,
  7 symbols) in ~18 min; the Oct 2026 recovery rerun (Aug 1–Oct 23) loaded
  **846,720 rows** and replaced the table atomically. The Market tab now has a
  real 30-day trend and dense 24 h detail, and today's live tail wins over the
  archive when both exist. Trade-offs: the archive is T+1 (today is live-only
  until the next daily file), `volatility` cannot be reconstructed from klines
  so that chart stays on the live table, and XRP (dropped from config) has no
  archive rows. Backfill is manual (`make backfill`), documented in the runbook.

## ADR-027 — Readable default view: BTC, 24 h detail + 30 d trend

- Context: the multi-symbol candlestick put coins with very different price
  scales on one axis (unreadable), the 1 m charts defaulted to 7 days (too dense),
  and 50-unit tiles with top legends/axis titles made labels collide with the
  plot.
- Decision:
  - The native **Symbol** filter ships a `defaultDataMask` for the first
    configured coin (`TICKSTREAM_SYMBOLS[0]`, BTC), so the dashboard opens on BTC;
    multi-select stays available. The filter's value list comes from the unified
    dataset, and it scopes every Market chart (`live` + `kline` datasets).
  - Price/VWAP, base volume, USD notional, trades/min and the candlestick use the
    **unified** dataset over the last **24 h at 1 m**; a new "price — 30d trend
    (1h)" line uses `now-30d : now`. Volatility remains live-only.
  - Chart tiles are 70 units (wide tables/candlestick) or 55 (others), with
    `x_axis_title_margin=30`, `y_axis_title_margin=60` and `legendMargin=4`.
- Consequence: the default screen is a readable BTC 24 h detail plus a 30 d trend,
  and labels no longer sit on top of the plotted series. Trade-off: the default
  coin is the first entry of `TICKSTREAM_SYMBOLS` (re-provision after reordering);
  the 24 h detail still inherits live-ingestion gaps for the current day.

## ADR-028 — Explicit symbol→partition routing for `trades.raw`

- Context: `trades.raw` has 3 partitions and the producer keyed by symbol with
  Kafka's murmur2 partitioner. In Oct 2026 six of the seven coins hashed to
  partition 0 (BTC+ETH ≈ 74% of all trades). One consumer is capped at ~240
  msg/s (20k-row merge ≈ 60–85 s), so that partition's lag grew and BTC/ETH
  gold candles fell ~1 h behind while the other two consumers idled.
- Decision: keep 3 partitions and key=symbol, but add an optional
  `TICKSTREAM_SYMBOL_PARTITIONS` JSON map (symbol → partition) applied when the
  producer sends. Unmapped symbols keep the hash. Invalid indexes fail at
  producer start. Current pinning: BTC→1, ETH/DOGE/LINK→2, SOL/ADA/AVAX→0.
  Key=symbol is preserved, so each symbol still lands on exactly one partition
  (per-symbol ordering and single-writer ownership for the Iceberg merge).
- Consequence: no topic change and no offset reset; new BTC traffic is fresh
  immediately and the 440k-message partition-0 backlog drains in ~35 min at
  ~195 msg/s while new traffic there is light. The drained rows land in bronze
  late, so a one-off silver/gold catch-up with wider lookbacks is needed
  afterwards. The map is volume-dependent: revisit it when the symbol mix or
  trade rates change materially. The `@ticker` stream added later uses its own
  topic and consumer (ADR-029), so it does not change this routing.

## ADR-029 — Separate `@ticker` stream for top-of-book microstructure

- Context: trades only show executed price/size; they cannot measure quoted
  spread, displayed depth or book imbalance. Binance's `@ticker` stream gives a
  1 Hz snapshot of best bid/ask (`b/B/a/A`), last trade (`c/Q`) and rolling 24 h
  stats for every symbol on the same market-data mirror.
- Decision: keep it off the trade hot path.
  - A second producer process (`ticker-producer`) opens its own combined
    `<symbol>@ticker` connection and writes `ticker.raw` (3 partitions,
    key=symbol). A second consumer (`ticker-consumer`, one replica — ~5–7 msg/s)
    MERGEs into `bronze_tickers`, partitioned by event day + symbol and
    idempotent on `(symbol, event_time)`.
  - The GX gate validates the ticker table over the same window (unique
    symbol+time, positive quote prices, freshness) and any failure blocks dbt,
    exactly like trades. The table is skipped only if it does not exist yet.
  - dbt: `stg_tickers` → `silver_tickers` (2 h lookback) →
    `gold_market_microstructure_1m` (1-minute avg spread bps, mid, displayed
    bid/ask quantity, imbalance in [-1, 1], last price, snapshot count; 30-min
    recompute). Two charts on the Market tab, scoped by the Symbol filter.
- Consequence: quoted spread/depth are queryable next to realized prices with
  negligible load (~5–7 msg/s, ~500-row batches, ~20–25 s commits measured
  under concurrent Glue metadata updates). The trade producer's
  reconnect/partition logic is untouched. Trade-off: a snapshot is
  keyed to the event second, so two updates within the same millisecond would
  collapse; Binance emits at most one per second per symbol. `@ticker` is a
  1 Hz sample, not a full book — depth beyond the top level is out of scope.

## ADR-030 — Start the batch window after the whole flush

- Status: accepted; amends ADR-014 (which had moved the window after the
  write + commit but not after the ops-metrics append).
- Context: ADR-014 fixed the self-perpetuating 1-row batch by moving
  `start_window()` out of `drain()` and after the write + offset commit. The
  flush still did one more blocking step afterwards: `ops.append_metrics()`
  writes a row to the Iceberg `pipeline_metrics` table, which took up to ~40 s
  when four consumers were committing metadata concurrently. If the whole flush
  (write + commit + metrics) exceeded the 45 s window, the next window was
  already expired when `_flush()` returned, so the consumer again flushed one
  message at a time. It recurred in Oct 2026 on the ticker consumer (one row per
  ~40 s, lag growing without bound) and looked like a stuck client rather than
  control flow.
- Decision: reset the window at the very end of `_flush()`, after the
  best-effort metrics append (which catches and logs its own failures). The
  invariant is "start timing the next batch when the consumer is actually ready
  to collect it", not "after the write" or "after the commit". Metrics failures
  are still swallowed; only their latency is now inside the completed cycle
  rather than before the reset.
- Consequence: trade and ticker batches return to full size (ticker ~500 rows,
  trade replicas 250–2,200 rows at commit) and lag stays bounded (~100–500
  in-flight). The tradeoff is that a slow metrics append delays the next batch
  just as it did before — but it can no longer shrink every subsequent batch.
  The batcher unit tests cover window semantics; the caller ordering is verified
  live (batch rows, lag, and the absence of `rows: 1` in `batch_committed`).
