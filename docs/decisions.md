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
