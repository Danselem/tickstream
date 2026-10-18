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

### ADR-012a — Dashboard↔chart linkage on Superset 6.0 (and the `+` in chart names)
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
  - The scheduler runs it on the **maintenance cadence** (every N cycles, ≈12 h),
    *before* dbt; `duplicates > 0` fails the cycle and blocks dbt (Brief §6),
    matching the "a failure stops the run" rule.
  - Exposed ad hoc as `make integrity` (`--integrity-only`).
- Consequence: keeps the frequent/cheap windowed gate AND a whole-table safety
  net, with no extra Athena spend (~19 s to scan 4.3M key rows). Trade-off: the
  whole-table scan is O(table) in Python, so it is deliberately NOT run every
  10 min; a duplicate could persist up to one maintenance interval before the
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
