# Design decisions (ADRs)

**What this answers:** why the non-obvious choices were made, what else was
considered, and what each choice costs. ADR means Architecture Decision Record.

These are short decision records, not tutorials. Terms are defined in the
[glossary](glossary.md). Each record has the same shape: status, context, decision,
why this option, and consequence. Accepted decisions are not silently rewritten.
When a decision changes, a new ADR supersedes or amends the old one and the index
status is updated. `ADR-012a` remains at its historical position.

> **Clarity edits:** wording may be tightened, but the decision, evidence, heading
> text, and anchor stay unchanged. A change in meaning requires a new ADR.

## Reading guide

| If you care about… | Read |
|---|---|
| Replays, duplicates, and crash safety | ADR-001, 013, 014, 015, 016, 023, 030 |
| Throughput and routing | ADR-004, 006, 013, 014, 028, 029 |
| Data quality and table design | ADR-002, 008, 009, 017, 019, 021, 024 |
| Dashboards and filters | ADR-011, 012, 012a, 018, 020, 022, 027 |
| Recovery and history | ADR-023, 025, 026 |
| Infrastructure and security | ADR-003, 004, 005, 010 |

## Index

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
| [ADR-022](#adr-022--trailing-time-windows-for-provisioned-charts-now-nd--now) | Trailing time windows | Accepted; amended by ADR-027 |
| [ADR-023](#adr-023--repair-bronze-duplicates-server-side-delete-both--insert-one) | Repair bronze duplicates server-side | Accepted |
| [ADR-024](#adr-024--incremental-windows-anchor-on-the-target-with-overridable-lookbacks) | Target-anchored incremental windows | Accepted |
| [ADR-025](#adr-025--treat-a-silent-websocket-as-dead-message-watchdog) | Treat a silent WebSocket as dead | Accepted |
| [ADR-026](#adr-026--archive-1m-klines-backfill-for-chart-history-bronze_klines) | Archive 1m klines backfill | Accepted |
| [ADR-027](#adr-027--readable-default-view-btc-24-h-detail--30-d-trend) | Readable default view | Accepted |
| [ADR-028](#adr-028--explicit-symbolpartition-routing-for-tradesraw) | Explicit symbol→partition routing | Accepted |
| [ADR-029](#adr-029--separate-ticker-stream-for-top-of-book-microstructure) | Separate `@ticker` stream | Accepted |
| [ADR-030](#adr-030--start-the-batch-window-after-the-whole-flush) | Start the batch window after the whole flush | Accepted |

## ADR-001 — Bronze idempotency via upsert, not append-only

- **Status:** superseded on the write path by ADR-013. The merge key
  `(symbol, trade_id)` and the “raw means no enrichment” definition still hold.
- **Context:** Kafka delivers at least once, so the same trade can arrive twice.
  The project also says bronze is raw and never modified. Those rules conflict
  unless “raw” is defined carefully.
- **Decision:** write bronze with an update-or-insert (upsert) on
  `(symbol, trade_id)`. “Raw” means the trade facts are not transformed or
  enriched; the upsert is only a duplicate-absorbing commit. Silver deduplicates
  again as a second line of defence.
- **Why:** a plain append would make every replay a duplicate. Skipping bronze
  entirely would lose the simple landing layer that makes reprocessing possible.
- **Consequence:** replays do not create duplicates, and bronze is unique at
  write time. A replay integration test guards this. The full-table upsert was
  later replaced for performance, not for a change in the key.

## ADR-002 — Great Expectations validates a local PyIceberg scan

- **Status:** accepted.
- **Context:** quality checks must run before every scheduled dbt build. Running them in
  Athena is realistic but costs money per query. The same basic checks can run on
  the new bronze window in Python.
- **Decision:** read the window with PyIceberg, convert it to a pandas dataframe,
  and run the GX checkpoint locally. The suite checks schema, nulls, ranges,
  uniqueness, row count, and freshness.
- **Why:** quality should be frequent and cheap. A local scan takes seconds and
  adds no Athena spend.
- **Consequence:** GX adds no Athena query spend, but it does not exercise the
  Athena path. dbt and Superset remain the checks that prove Athena works.

## ADR-003 — OpenTofu state in S3 + DynamoDB, bootstrapped once

- **Status:** accepted.
- **Context:** infrastructure state should be remote and locked so two applies
  cannot race. That requires a bucket and lock table before the main stack exists
  — a chicken-and-egg problem.
- **Decision:** a small `infra/bootstrap/` configuration creates the state bucket
  and DynamoDB table with local state. `make infra-bootstrap` writes
  `infra/backend.hcl`. The main stack then uses encrypted remote state with a
  committed lock file.
- **Why:** local state is fragile and unsuitable for a portfolio demonstration of
  real infrastructure practice. A managed backend would add another dependency.
- **Consequence:** setup has one extra one-time command. After that, all state is
  remote and locked.

## ADR-004 — Single-broker KRaft Kafka, RF=1

- **Status:** accepted.
- **Context:** the project runs on one machine at a volume a single broker can
  handle. Multiple brokers would add cost and operational steps without proving
  anything new here.
- **Decision:** use the official `apache/kafka` image in combined broker/controller
  mode (KRaft), with replication factor 1 everywhere, including the consumer
  offsets topic.
- **Why:** simple and sufficient for the workload. A real production design would
  use at least three brokers, RF=3, `min.insync.replicas=2`, and transactions for
  exactly-once delivery.
- **Consequence:** the broker is a single point of failure: a host stop takes the
  pipeline down. This is documented, not hidden. Kafka data survives a normal
  Docker restart because it is on a volume.

## ADR-005 — Real AWS only; offline covered by pytest, not by compose

- **Status:** accepted.
- **Context:** the brief says everything except S3, Glue, and Athena runs locally.
  An offline MinIO/SQLite compose profile would add services and drift from the
  real system.
- **Decision:** Docker Compose always points at real AWS. The catalog is
  configurable (`glue` or `sql`), and integration tests use the SQL catalog plus a
  temporary warehouse for a fully offline path.
- **Why:** tests should be deterministic and add no AWS spend; the running system
  should use the real services it claims to use.
- **Consequence:** ingestion development needs AWS credentials. Tests do not, so
  CI stays offline.

## ADR-006 — Kafka topic: 3 partitions, symbol keys, 7-day retention

- **Status:** accepted. The “single consumer” assumption is superseded by
  ADR-014; topic size, key, and retention are unchanged.
- **Context:** the project began with one symbol and needed room for seven.
  Replays must be possible after a short outage. Partition count cannot be reduced
  later without recreating the topic.
- **Decision:** `trades.raw` has three partitions, key `symbol`, and seven-day
  retention. The producer creates the topic at startup. Two listeners are used:
  `kafka:19092` inside Compose and `localhost:9092` on the host.
- **Why:** three partitions allow three consumers and preserve per-symbol order.
  Seven days gives useful replay headroom without keeping data forever.
- **Consequence:** symbols originally hash to partitions; later routing is
  explicit (ADR-028). Restart and SIGKILL tests produced no duplicates. Empty
  partitions briefly over-reported lag; that was fixed in `metrics.py` with a
  regression test.

## ADR-007 — One micro-batch commit = one Iceberg snapshot/file (for now)

- **Status:** accepted; addressed later by scheduled maintenance.
- **Context:** a 45-second batch creates one small Iceberg file per commit. At
  the start that was about 14 files in 10 minutes — fine, but it becomes the
  small-files problem as volume grows.
- **Decision:** accept the small files during the initial build, then add
  scheduled Athena `OPTIMIZE` and `VACUUM` rather than changing the write
  mechanism immediately.
- **Why:** per-event writes would be worse, and a complex streaming writer would
  slow delivery. Compaction is a known, low-risk remedy.
- **Consequence:** file count needs routine maintenance. `OPTIMIZE` merges files
  and `VACUUM` removes old snapshots; both now run every 72 cycles.

## ADR-008 — Ops tables live in the `tickstream` Glue database

- **Status:** accepted.
- **Context:** the plan named `ops.quality_results` and `ops.pipeline_metrics`, but
  infrastructure creates one Glue database, `tickstream`, and the pipeline user
  cannot create databases.
- **Decision:** keep observability tables in the same database:
  `tickstream.quality_results`, `pipeline_metrics`, `integrity_results`,
  `layer_counts`, and `symbol_freshness`. They are unpartitioned, append-only
  Iceberg tables created idempotently by `ensure_ops_tables()`.
- **Why:** no infrastructure change or extra IAM permission is needed, and the
  tables are easy to query alongside the data.
- **Consequence:** metrics are appended after the offset commit, so a crash in that
  short gap may lose one metrics row; a replay does not duplicate it. Bronze
  correctness is unaffected.

## ADR-009 — dbt MERGEs carry an `update_condition` (immutable-event pattern)

- **Status:** accepted.
- **Context:** trade facts do not change, but silver's two-hour lookback re-reads
  about 60,000 rows per run. Updating every matched row rewrote identical data and
  twice caused Glue `UpdateTable` timeouts.
- **Decision:** add a null-safe `update_condition` using `IS DISTINCT FROM` over
  all non-key columns on silver and gold. Matched-but-identical rows are skipped;
  genuinely changed rows still update. GX uniqueness uses a derived
  `symbol:trade_id` value because trade IDs are only unique within a symbol.
- **Why:** the write should reflect change, not activity. Skipping identical rows
  is correct for immutable events and much cheaper.
- **Consequence:** incremental merges commit in seconds. A second build merged only
  new keys and produced zero duplicates.

## ADR-010 — One IAM addition for dbt: `glue:GetTableVersions`

- **Status:** accepted.
- **Context:** the first `dbt build` failed because dbt-athena calls
  `GetTableVersions` when materialising relations, and the least-privilege policy
  did not include it.
- **Decision:** add that single read-only action to `infra/iam.tf`. The change was
  planned, reviewed, and applied with approval.
- **Why:** the adapter needs the action; broadening the whole policy would hide
  the real requirement.
- **Consequence:** dbt builds are green. The incident also exposed a trap: always
  re-plan with the real `ALERT_EMAIL`, because the placeholder default can replace
  the real budget notification address.

## ADR-011 — Superset dashboard as code via a REST provision script

- **Status:** accepted.
- **Context:** the original plan was to commit an exported dashboard ZIP. There is
  no browser in the setup loop, and hand-writing the export format (unique IDs,
  layout nodes) is harder to review than calling the REST API.
- **Decision:** `src/pipeline/superset/provision.py` uses the Superset REST API to
  create or update the Athena database, datasets, charts, and dashboard. It runs
  at container boot and through `make superset`. It is idempotent and unit-tested
  with a fake transport. Datasets use gold and ops tables, never bronze.
- **Why:** API calls are explicit, testable, and reproducible. A ZIP is an opaque
  binary-ish artifact that drifts silently.
- **Consequence:** `make up` ends with a published dashboard and no manual chart
  clicks. The first version had five charts; it now has 20. SQL Lab was used to
  confirm live row counts.

## ADR-012 — Superset-on-Athena gotchas (config, drivers, IAM, catalog case)

- **Status:** accepted.
- **Context:** five independent failures each looked like something else:
  1. `SUPERSET_CONFIG` expects a Python module name, not a file path. The custom
     module replaces `superset.config`, so it must start with
     `from superset.config import *` or fail with `KeyError: 'DATA_DIR'`.
  2. The image runs from `/app/.venv` without system pip. Installing `pyathena`
     with system pip puts it outside `sys.path`, so every database connection
     fails. The driver install must use `ensurepip` and the venv pip.
  3. Mutating API calls need both the session cookie and `X-CSRFToken` (a
     cross-site request-forgery token), even with a JWT bearer token (a signed API
     token).
  4. Athena metadata calls authorise against the data catalog, not the workgroup.
     `GetTableMetadata` and `ListDatabases` need `datacatalog/AwsDataCatalog`
     scope; a workgroup-only grant is silently insufficient.
  5. PyAthena defaults the catalog name to lowercase `awsdatacatalog`, but Athena
     matches catalog names case-sensitively. Pin `catalog_name=AwsDataCatalog`.
- **Decision:** record all five. Diagnose with the same principal, CloudTrail, and
  the driver source rather than guessing.
- **Why:** these failures waste hours because the surface symptoms are generic
  connection or login errors.
- **Consequence:** dataset sync and SQL Lab work, and the dashboard reads live
  data. The notes are the checklist for the next driver or image upgrade.

## ADR-012a — Dashboard↔chart linkage on Superset 6.0 (and the `+` in chart names)

- **Status:** accepted.
- **Context:** every dashboard tile showed “There is no chart definition
  associated with this component”. In Superset 6.0 the dashboard write schema has
  no `charts` field; the `dashboard_slices` association is populated only when
  `json_metadata` contains `positions`. The provisioner sent `position_json`, not
  `json_metadata`, so the association stayed empty. Separately, chart lookup used
  the list API's `slice_name eq` filter, which returns nothing for names
  containing `+` (“Consumer lag + batch rows”). That chart was recreated on every
  run, leaving 11 duplicates.
- **Decision:** send `json_metadata = {"positions": layout}` on the dashboard PUT
  and always issue a PUT after create. Match charts by listing them and comparing
  `slice_name` in Python, as the dataset helper already did.
- **Why:** rely on the API's documented write path and avoid a filter that silently
  returns no rows for valid names.
- **Consequence:** tiles render, associations persist, and chart count is stable
  across runs. Verified by reading the metadata database and running
  `make superset` twice.

## ADR-013 — Bronze write path: pruned-scan merge, transient retry, S3 timeouts

- **Status:** accepted; replaces the full-table upsert on the write path.
- **Context:** three problems stacked after the 7-coin launch:
  1. `table.upsert` merged against the whole table in Python. Work grew with table
     size, so a 20k batch crawled at 100% CPU while a ~1M backlog built up.
  2. Transient S3 socket timeouts, caused by short pyarrow defaults on degraded
     Docker networking, crashed the consumer. Each crash rebalanced the group and
     reset progress, creating a crash/rebalance loop.
  3. The default `max.poll.interval.ms` (5 minutes) was shorter than a retry
     storm, so Kafka rebalanced even when the process stayed alive.
- **Decision:** scan only the batch's day and symbol scope, anti-join in
  Arrow/Python on `(symbol, trade_id)`, append only new rows, and skip the commit
  when nothing is new. Add tenacity retries for transient S3/Botocore/PyIceberg
  commit errors, set S3 connect/request timeouts to 15/60 seconds, and raise
  `max.poll.interval.ms` to 10 minutes.
- **Why:** correctness stays the same — no duplicate keys, replay-safe — while the
  work becomes proportional to the batch and its partition scope, not the whole
  table. Manifest pruning means Iceberg skips files outside the batch range.
- **Consequence:** the first merged batch scanned 20k rows, appended 2, and skipped
  19,998. A backlog drains at about 20k rows per batch, and fresh symbols land
  while it drains. A slow metrics write remains best-effort and cannot break the
  data path.

## ADR-014 — Consumer scale-out to 3 replicas + batching-window fix

- **Status:** accepted; amended by ADR-030 for metrics ordering.
- **Context:** one consumer could not keep up with seven streams. Diagnosis found
  three separate problems, only one about capacity:
  1. **The real stall:** `TradeBatcher.drain()` reset the 45-second window at the
     start of a flush. When a flush took longer than 45 seconds, the next window
     was already expired, so the consumer flushed one message at a time — forever.
  2. **Stuck client after churn:** 228 restarts from a DNS/clock incident left a
     consumer polling almost nothing until a clean restart. librdkafka itself was
     healthy when probed directly.
  3. **Capacity:** one consumer over three partitions could not match seven
     streams.
- **Decision:** start the window after the write and commit, not inside `drain()`;
  add a regression test; run three consumer replicas, one per partition, in the
  same group; remove the fixed `container_name` so Compose can manage replica
  names; and raise the WebSocket keepalive to match Binance's ping/pong contract.
- **Why:** keying by symbol means each symbol is owned by exactly one partition
  and one replica. Parallel writers never touch the same key, while PyIceberg's
  optimistic commit retry handles metadata contention.
- **Consequence:** offsets advance at about 20k rows per batch and a ~1M backlog
  drains in about an hour. A partition with a much larger historical backlog
  drains more slowly until the imbalance evens out. The “single-consumer group”
  rationale in ADR-013 is superseded for scaling, but the 10-minute poll bound
  remains.

## ADR-015 — Cross-replica duplication on rebalance; refresh-before-scan + revoke commit

- **Status:** accepted.
- **Context:** after scaling to three replicas, bronze gained **15,825 duplicate
  `(symbol, trade_id)` keys**, clustered at the restart. GX uniqueness would fail
  and block dbt.
- **Root cause:** each process cached its own Iceberg `Table` object and saw only
  its own appends. On a rebalance, the new owner replayed a batch the old owner
  had already written, but its cached metadata predated that write. The
  partition-scoped anti-join found nothing and appended the rows again.
- **Decision:** call `table.refresh()` before the anti-join scan. Use
  cooperative-sticky assignment (rebalances move only the partitions that need to
  move) and an `on_revoke` handler that flushes and commits the in-flight window
  before ownership moves. Repair the existing damage
  once with `make dedup-bronze`, with consumers stopped.
- **Why:** the cached view was the bug, not the merge key. Refreshing closes the
  normal rebalance window, while cooperative assignment and commit-on-revoke make
  replays rarer and smaller.
- **Consequence:** a regression test runs two writers with one stale handle. A
  truly simultaneous double-write would still need a row-level merge, which is
  avoided because PyIceberg's partitioned upsert has a known bug. Residual
  duplicates are caught by GX and the whole-table integrity check. The dedup
  repair is one of two sanctioned exceptions to “bronze is never modified” (the
  other is the kline-table replacement in ADR-026); it restores the invariant the
  gate depends on.

## ADR-016 — Whole-table integrity check on the maintenance cadence

- **Status:** accepted.
- **Context:** the windowed GX uniqueness check only sees recent data. A duplicate
  introduced outside that window, for example by a stale handle replaying an old
  batch, can hide indefinitely. The 15.8k-row incident was found by a manual
  full-table query, not by the scheduled run.
- **Decision:** add a separate whole-table check on the maintenance cadence.
  `quality/integrity.py` scans only `symbol` and `trade_id` with local PyIceberg,
  computes `duplicates = rows − distinct_keys`, and appends a row to
  `tickstream.integrity_results`. Any duplicate fails the cycle and blocks dbt.
- **Why:** frequent checks should stay cheap; a coarse safety net can run rarely
  and still catch what the window misses.
- **Consequence:** the system now has both a cheap windowed gate and a whole-table
  backstop. Scanning 4.3M key rows took about 19 seconds and cost $0 in Athena.
  A duplicate could persist for up to one maintenance interval (~1.5–2 days)
  before the next check, which is the accepted trade-off.

## ADR-017 — Business gold summary + tabbed dashboard (two audiences)

- **Status:** accepted.
- **Context:** the dashboard mixed engineering signals with raw price charts, and
  had no business metrics. A manager needs USD notional, range, % change, and
  trades per minute; an on-call engineer needs lag, quality, and integrity. One
  grid served neither.
- **Decision:** put business math in the gold layer and split the dashboard into
  **Market** and **Pipeline health** tabs. Add `notional = sum(price*quantity)` to
  candles and build daily/hourly summaries from one shared macro, with open,
  close, high, low, volume, trade count, trades/min, and
  `change_pct = (close-open)/open`. Recompute summaries on a trailing window.
- **Why:** aggregation belongs in tested SQL, not chart formulas. Superset has no
  simple first/last aggregate for `% change`, so a summary model is the reliable
  place.
- **Consequence:** each audience gets a focused view and one source of truth.
  Adding `notional` to an existing Iceberg table needed a one-off
  `make dbt-refresh` because schema changes are not automatic. The summaries are
  included in maintenance.

## ADR-018 — Dashboard readability: explicit labels, legends, and stable tab ids

- **Status:** accepted.
- **Context:** provisioned charts rendered as unlabelled lines. Axes, legends, and
  time formatting relied on defaults; tables showed raw column names; and lag
  (messages) shared an axis with batch rows (count), which is misleading. Tab IDs
  were index-based, so moving a tab could silently detach a filter.
- **Decision:** make readability part of the chart specification. Set axis titles,
  scrollable top legends, time formats, rich tooltips, and colour schemes for
  every time series. Label columns through one map that resends the full column
  list with IDs, types, and datetime flags. Split “lag + rows” into two charts.
  Derive tab IDs from titles so filter scopes stay stable.
- **Why:** a chart with no labels answers no question, and UI-only fixes are lost
  when the dashboard is rebuilt.
- **Consequence:** charts are self-describing and safe to reorder, and the settings
  are unit-tested code. The trade-off is that the label map must be extended when
  a new column is exposed.

## ADR-019 — Dimension views (`dim_symbol`, `dim_time`)

- **Status:** accepted.
- **Context:** gold was made of event tables with no dimensions. A symbol
  dimension and a time spine would help later grouping, but renaming every table to
  a `fct_*` convention would churn models, tests, and docs for little benefit.
- **Decision:** add two views and keep the fact names. `dim_symbol` parses base
  and quote assets and records first/last seen. `dim_time` builds a bounded minute
  spine with hour, day, weekday, and hour-of-day attributes.
- **Why:** dimensions are useful; a mass rename is not. Views add no storage.
- **Consequence:** there is a clean dimension layer without migration. `dim_time`
  is recomputed per query and is not yet wired to a chart. A stored table would be
  the next step if query cost justified it.

## ADR-020 — Market overview + native dashboard filters

- **Status:** accepted.
- **Context:** the Market tab had time series but no at-a-glance summary, and every
  chart plotted all seven coins. Viewers needed a candlestick, proportional
  volume, headline figures, and a way to focus one symbol.
- **Decision:** add a native candlestick, a notional-share treemap, and two KPI
  headline-number tiles. Add a **Symbol** select filter and a global **Time range**
  filter in `json_metadata`. Send positions and filters in one PUT because the dashboard PUT
  replaces metadata wholesale.
- **Why:** the overview should answer “what is happening now” before the detailed
  series, and filters should be native so every chart stays consistent.
- **Consequence:** the dashboard can be focused per coin while summary tables keep
  all coins. Chart keys are Superset-6-specific (`treemap_v2`, `big_number_total`,
  `candlestick`), so upgrades need verification. Filters target datasets by UUID,
  and provisioning fails loudly if a UUID is missing. Candlestick correctness
  assumes the 1-minute grain.

## ADR-021 — Lake-level health: per-layer row counts and per-symbol freshness

- **Status:** accepted.
- **Context:** Pipeline health showed Kafka and quality signals but not whether
  rows actually landed in bronze, silver, and gold, or how stale each symbol was.
  A stuck layer would only surface indirectly, later.
- **Decision:** compute two health facts once per maintenance cycle:
  per-layer row counts and per-symbol freshness. Prefer Iceberg's constant-time
  snapshot record count; fall back to a one-column scan. Append to `layer_counts` and
  `symbol_freshness`, and chart both. Run inside maintenance and expose
  `make health`.
- **Why:** the gate looks at one recent window; health needs a broader, cheap
  snapshot across layers.
- **Consequence:** the dashboard answers “is data landing and is it fresh”, and
  the tables are queryable for future alerts. Counts are a maintenance snapshot,
  not continuous. The fallback scan takes about 50 seconds, which is acceptable
  off the hot path.

## ADR-022 — Trailing time windows for provisioned charts (`now-Nd : now`)

- **Status:** accepted; amended by ADR-027 for detail and trend charts.
- **Context:** charts used Superset's “Last day” preset. Named presets are anchored
  to midnight and exclude the current day, so rows written today rendered as no
  data and “today” headline numbers silently showed yesterday.
- **Decision:** use explicit trailing ranges relative to now. Ops and multi-day
  series use `now-7d : now`; summary tables, the treemap, and headline-number tiles
  use `now-1d : now`. ADR-027 later narrows the 1-minute detail view to `now-1d`
  and adds a `now-30d` hourly trend. Each chart carries its own range, and labels
  say “last 24h” rather than “today”.
- **Why:** the newest point must be visible, and the title must match the window.
- **Consequence:** the dashboard always includes current data. The trade-off is
  that exported SQL and screenshots differ by day because the range is evaluated
  per request.

## ADR-023 — Repair bronze duplicates server-side (`DELETE` both + `INSERT` one)

- **Status:** accepted.
- **Context:** one duplicate key (`BTCUSDT:6774376484`, two identical rows) in
  1.7M rows made the GX uniqueness gate fail forever. The gate only advances on
  success, so it retried the same bad window. A local PyIceberg rewrite over the
  uncompacted S3 layout ran for more than 15 minutes without committing.
- **Decision:** repair that key in Athena: read the row, delete both copies by key,
  then insert one back. This is safe after the delete because the key is unique and
  the row is deterministic. The PyIceberg path stays for small or compacted tables
  and offline tests.
- **Why:** Athena can prune by partition and file statistics, while the local
  rewrite had to move far more data.
- **Consequence:** the repair committed in about 40 seconds (2 → 0 → 1 row), and
  the gate passed on 1.85M rows. It is a manual two-statement procedure with
  consumers stopped, documented in the runbook. The durable fix is compaction.

## ADR-024 — Incremental windows anchor on the target, with overridable lookbacks

- **Status:** accepted.
- **Context:** gold's incremental filter started from the maximum event time in
  `silver_trades`, the source, not the gold table. After a skipped run the source
  was current, so the next build recomputed only the last 30 minutes and left a
  permanent hole. Fixed 2-hour/30-minute lookbacks could not heal longer gaps.
- **Decision:** anchor silver on `{{ this }}`'s maximum event time and gold on the
  maximum candle start. Make lookbacks dbt variables: `silver_lookback_hours`
  (default 2) and `gold_lookback_minutes` (default 30). A one-off run with larger
  values repairs a gap.
- **Why:** recovery should start where the target stopped, not where the source
  happens to be. Variables allow a deliberate catch-up without code changes.
- **Consequence:** a skipped run is caught up on the next success. A long outage
  can be healed with wider windows, for example 120 hours for silver and 7,200
  minutes for gold. The cost is a larger scan on recovery, still bounded and
  idempotent through the merge keys.

## ADR-025 — Treat a silent WebSocket as dead (message watchdog)

- **Status:** accepted.
- **Context:** the keepalive only detects a dead TCP path. In October 2026 the
  combined stream stayed open and answered pings but delivered no trades for
  hours. Bronze lost whole hours, and throughput was logged only when a message
  arrived, so the silence was invisible. The 60-second ping timeout never fired.
- **Decision:** wrap `ws.recv()` in `asyncio.wait_for` with
  `ws_stale_after_s` (default 60 seconds). On timeout, raise `StaleStreamError`,
  log `ws_stalled`, and reconnect through the existing backoff. Keep the keepalive
  for dead TCP.
- **Why:** a healthy connection that carries no data is still unusable. The
  watchdog measures the thing that matters: messages, not pings.
- **Consequence:** a silent subscription recovers in about a minute instead of
  hours. Sixty seconds is abnormal for seven active symbols; a quiet symbol set
  would need a higher threshold. Candle holes are repaired by archive backfill;
  trade and quote gaps are not, because there is no archive for them.

## ADR-026 — Archive 1m klines backfill for chart history (`bronze_klines`)

- **Status:** accepted.
- **Context:** the live lake held only about two days of usable candles, with gaps
  from the silent-WebSocket incident, so 7-day and 30-day charts were mostly
  empty. Binance publishes 1-minute kline archives monthly for complete months and
  daily for the current month. It has no historical order-book archive, so klines
  are the cheap way to rebuild OHLCV.
- **Decision:** build an offline backfill instead of replaying trades. Download
  the zip files, verify each SHA256 sidecar, parse the headerless CSV, normalise
  microsecond timestamps to naive UTC, and write `bronze_klines` keyed by
  `(symbol, open_time)`. Default to the first day of the month 60 days ago through
  yesterday. `FORCE=1` replaces the whole table in one atomic commit; without it,
  an existing table is left alone.
- **Why:** replaying months of trades would be slower and still would not include
  minutes the live pipeline never received. Archives are the authoritative cheap
  source for candles.
- **Consequence:** the first load covered 806,400 candles (1 Aug–19 Oct 2026) in
  about 18 minutes. The recovery rerun loaded **846,720 rows** for 1 Aug–23 Oct and
  replaced the table atomically. `gold_candles_1m_unified` lets live candles win
  and archive candles fill gaps. Limits: the archive is T+1, volatility cannot be
  rebuilt from klines, and XRP has no archive rows after being removed from config.
  The backfill is manual and gated by checksums plus dbt tests, not the live GX
  window.

## ADR-027 — Readable default view: BTC, 24 h detail + 30 d trend

- **Status:** accepted.
- **Context:** the multi-symbol candlestick put coins with very different prices on
  one axis, which was unreadable. The 1-minute charts defaulted to seven days,
  which was too dense. Large tiles with top legends and axis titles made labels
  collide with the plot.
- **Decision:** default the Symbol filter to the first configured coin (BTC), use
  the unified dataset for 24-hour 1-minute detail, add a 30-day hourly trend, and
  use larger tiles with explicit axis and legend margins. Keep volatility live-only.
- **Why:** a default view should be readable before the viewer touches a filter.
  Separating detail from trend shows both without cramming them onto one axis.
- **Consequence:** the dashboard opens on a readable BTC view. Reordering
  `TICKSTREAM_SYMBOLS` changes the default, so re-provision after reordering. The
  24-hour detail still inherits live-ingestion gaps for the current day.

## ADR-028 — Explicit symbol→partition routing for `trades.raw`

- **Status:** accepted.
- **Context:** the producer keyed by symbol and let Kafka hash to three partitions.
  Six of seven coins hashed to partition 0, with BTC and ETH alone at about 74% of
  trades. One consumer is capped near 240 messages/s, so partition 0's lag grew
  while the other consumers idled.
- **Decision:** keep three partitions and `key=symbol`, but add an optional
  `TICKSTREAM_SYMBOL_PARTITIONS` JSON map applied when producing. Unmapped symbols
  keep the hash, and invalid indexes fail at startup. Current map: BTC→1,
  ETH/DOGE/LINK→2, SOL/ADA/AVAX→0.
- **Why:** the key preserves per-symbol ordering and single-writer ownership; only
  the destination is controlled. Changing the topic or resetting offsets would
  lose history or create more work.
- **Consequence:** no topic change or offset reset. New BTC traffic became fresh
  immediately and the 440k-message partition-0 backlog drained in about 35
  minutes at ~195 messages/s. Rows that drained late required a one-off wider
  silver/gold catch-up. The map is volume-dependent and must be revisited when the
  symbol mix changes.

## ADR-029 — Separate `@ticker` stream for top-of-book microstructure

- **Status:** accepted.
- **Context:** trades show executed price and size, not quoted spread, displayed
  depth, or book imbalance. Binance's `@ticker` stream provides a 1 Hz snapshot of
  best bid/ask, last trade, and rolling 24-hour stats.
- **Decision:** keep quotes off the trade hot path. A second producer opens its own
  combined stream and writes `ticker.raw` with three partitions, keyed by symbol.
  A single ticker consumer merges into `bronze_tickers` on `(symbol, event_time)`.
  GX validates the same kind of window and blocks dbt on failure. dbt builds
  `silver_tickers` and a 1-minute microstructure model with spread, mid, depth,
  imbalance, last price, and snapshot count.
- **Why:** a separate path cannot slow trade ingestion, and the snapshot rate is
  low enough for one consumer.
- **Consequence:** quoted spread and depth sit next to realised prices at ~5–7
  messages/s; batches follow the shared 45 s window, and commits can take tens of
  seconds under concurrent Glue updates. Two charts use the Symbol filter. Limits: a snapshot
  is keyed to the event second, so two updates in the same second would collapse;
  Binance emits at most one per second. This is top-of-book, not a full order
  book, and historical quote data cannot be recovered.

## ADR-030 — Start the batch window after the whole flush

- **Status:** accepted; amends ADR-014, which had moved the window after the write
  and commit but not after the metrics append.
- **Context:** ADR-014 fixed the one-row batch loop by moving `start_window()` out
  of `drain()` and after the write and offset commit. The flush still did one more
  blocking step: `ops.append_metrics()` wrote to Iceberg and could take ~40 seconds
  when four consumers committed metadata together. If the whole flush exceeded 45
  seconds, the next window was already expired and the consumer returned to
  one-message batches. It recurred on the ticker consumer and looked like a stuck
  client rather than control flow.
- **Decision:** reset the window at the very end of `_flush()`, after the
  best-effort metrics append. The invariant is “start timing the next batch when
  the consumer is ready to collect it”, not “after the write” or “after the
  commit”. Metrics failures are still swallowed and logged.
- **Why:** the timer should measure collection time, not the previous flush. Any
  step inside the flush must finish before the next collection window begins.
- **Consequence:** trade and ticker batches return to full size (ticker rows follow
  the 45 s window at 5–7 msg/s; trade replicas 250–2,200 rows at commit), and lag
  stays bounded at roughly 100–500 in flight. A slow metrics append still delays the next batch, but it can
  no longer shrink every subsequent batch. Unit tests cover window semantics; the
  caller ordering is verified live by batch size, lag, and the absence of
  `rows: 1` in `batch_committed`.
