# Runbook

[Prerequisites](#prereqs) · [First-time setup](#first-time-setup) ·
[Daily commands](#daily-commands) · [Incidents](#incidents-and-how-they-were-fixed) ·
[Maintenance](#maintenance-and-compaction) · [Backfills](#kline-backfill-history-for-charts-adr-026) ·
[Host recovery](#after-a-host-stop-or-sleep-docker-daemon-down) · [Teardown](#tear-down)

## Prereqs

- Python 3.12 via `uv` (`uv python install 3.12`), Docker, OpenTofu ≥ 1.9, AWS CLI.
- AWS credentials for a user/role allowed to create the bootstrap + infra resources.

## First-time setup

1. `cp .env.example .env` and fill in values (never commit `.env`).
   Generate secrets: `openssl rand -hex 32` (Superset secret), strong admin password.
2. `make install` — installs deps, writes `uv.lock`.
3. `make lint typecheck test` — must be green before any other work.
4. `make infra-bootstrap` — one-time state backend (S3 + DynamoDB).
5. `make infra-plan ALERT_EMAIL=you@example.com` — review the output. Budget alert first.
6. `make infra-apply` — **only with explicit human approval.**
7. Create an access key for the `tickstream-pipeline` IAM user, put it in `.env`.
8. `make up` — producer, consumer, scheduler, Superset (dashboard auto-provisions
   at boot: http://localhost:8088, log in as the admin from `.env`).

## Daily commands

See `make help`. `make up` / `make down` / `make logs`; `make quality` /
`make dbt-build` / `make maintenance` / `make compact`; `make integrity`
(whole-table duplicate check; also runs automatically every 72 cycles);
`make health` (row counts + freshness); `make superset` re-runs dashboard
provisioning against the live container (idempotent). Integration tests are
opt-in: `RUN_KAFKA_TESTS=1 uv run pytest -m integration`.

**Images are baked, not mounted.** A code change needs
`docker compose build <service>` and
`docker compose up -d --no-deps --force-recreate <service>`. Each `build: .`
service has its own image: rebuilding `consumer` does **not** update
`ticker-consumer` (`tickstream-ticker-consumer`). Scaled consumers are
`tickstream-consumer-1/2/3`; read them with `docker logs tickstream-consumer-1`
(the `docker compose logs consumer-1` form does not exist).

## Dashboards

- URL http://localhost:8088 → dashboard `tickstream` (published).
- **Market** tab: candlestick, price/VWAP, volume, USD notional, trades/min,
  volatility, quoted spread, book imbalance, treemap, two KPI tiles, daily &
  hourly summaries (24 h detail + 30 d trend).
- **Pipeline health** tab: lag, batch rows, quality runs, bronze integrity, rows
  per layer, per-symbol freshness. Datasets: 10 total, never raw bronze.
- Native **Symbol** filter defaults to BTC and scopes the Market charts; **Time
  range** is global. XRP remains queryable but is excluded from the filter list.
- After changing `TICKSTREAM_SYMBOLS` or `provision.py`: rebuild the Superset
  image (`docker compose build superset`) and run `make superset`; the target
  executes the image's baked script, not the host file. If charts are empty
  after a fresh boot, gold has not been built yet — wait for the first
  GX → dbt cycle or run `make dbt-build`.

## Incidents and how they were fixed

Start with the symptom, then run the first check. The
[architecture](architecture.md#failure-modes-seen-in-practice-and-the-fixes)
doc explains why these failures happen.

| Symptom | First check | Fix |
|---|---|---|
| Consumer restarts every few minutes; `OSError curlCode: 28`; lag and rebalances | `docker compose logs consumer` and count retries | Transient-only retry, raised S3 timeouts, `max.poll.interval.ms` 10 min |
| `batch_committed` shows `rows=1`; offset advances by 1; `max_event_time` frozen | `docker logs tickstream-consumer-1` and look for `batch_committed` | Restart a librdkafka-stuck replica; otherwise inspect `_flush()` ordering (ADR-014/030). Reset only at the end of the flush |
| Lag is uneven or not draining | `kafka-consumer-groups.sh --describe --group bronze-writer` | Symbols are pinned (BTC→1, ETH/DOGE/LINK→2, SOL/ADA/AVAX→0); rebalance the map if the mix changes. Never scale above 3 |
| Kafka broker died (exit 137, host outage) | `docker compose up -d kafka`; check the `kafka-data` volume | Backlog drains at ~20k/batch; freshness self-heals once fresh timestamps commit |
| dbt Glue timeout on MERGE | Check the affected model and lookback | `update_condition` skips identical rows (ADR-009); suspect lookback size first |
| GX failing | `quality/reports/<latest>.json` and `failed_expectations` | Freshness failures heal after fresh data; duplicate-key failures use the dedup section below. A failed run does not advance the window |
| Superset 500s on dataset sync | `docker logs tickstream-superset` and the traceback | Almost always IAM (`GetTableMetadata`/`ListDatabases`) or catalog case (ADR-012) |
| Superset login fails after rebuild | Check the `superset-home` volume | Admin persists on the volume; if deleted, bootstrap recreates it from `.env` |
| Code change appears to have no effect | Check which image the service runs | Baked images, not host mounts. Rebuild and recreate the exact service; `ticker-consumer` is a separate image |
| Charts empty after a fresh boot | Check whether gold tables exist | Superset may provision before the first dbt build; wait for GX → dbt, or run `make dbt-build` then `make superset` |

## Maintenance and compaction

- The scheduler runs `OPTIMIZE … REWRITE DATA USING BIN_PACK` + `VACUUM` every
  72 cycles (~1.5–2 days at the current ~34 min cycle); `make maintenance` and
  `make compact` run the same work on demand.
- Do not run compaction during a backlog drain or a backfill; it races writers
  and wastes requests. `make compact TABLES=bronze_trades,silver_trades` narrows
  the scope.
- `OPTIMIZE pipeline_metrics` can fail with `ICEBERG_COMMIT_ERROR` while four
  consumers append metadata concurrently. Its `VACUUM` still succeeds and no
  data is affected; retry in a quiet window.
- After a large load or backfill, compact and then check object counts in S3.
  Small files, not bytes, drive the S3 request bill (see `docs/cost.md`).

## Add a symbol

1. Add the uppercase symbol to `TICKSTREAM_SYMBOLS` in `.env` and map it to a
   partition in `TICKSTREAM_SYMBOL_PARTITIONS` (keep the three partitions
   balanced; unmapped symbols fall back to the murmur2 hash).
2. `make restart` (rebuilds and recreates with the new env) and confirm
   `ws_connected` plus `batch_committed` for both streams.
3. `make superset` after the Superset image is rebuilt if `provision.py` changed;
   the Symbol filter picks up configured coins.
4. For history, run `make backfill FORCE=1` with the default full symbol list
   (`SYMBOLS` narrows the download but the table replacement covers only the
   selected symbols), then `make dbt-build`. The archive is T+1 and has no
   order-book data.

## Rotate secrets

- `SUPERSET_SECRET_KEY` (`.env`): replacing it invalidates existing sessions.
  Recreate the Superset container (`docker compose up -d --no-deps
  --force-recreate superset`), then log in again. Never commit `.env`.
- AWS access keys: create a new key for `tickstream-pipeline`, update `.env`,
  recreate the Python services, verify `make health`, then deactivate the old
  key. `.env.example` contains placeholders only.

## Bronze dedup procedure (one-time, ADR-015)

Only needed if bronze holds duplicate `(symbol, trade_id)`. Detected by the
scheduled whole-table integrity check (ADR-016, `duplicates > 0` blocks dbt and
shows in `tickstream.integrity_results`); the windowed GX uniqueness gate may
miss duplicates outside its window.

1. Confirm the damage first (0 expected):
   `SELECT count(*) c, count(DISTINCT symbol||'#'||CAST(trade_id AS varchar)) d
   FROM bronze_trades` — if `c > d`, there are duplicates.
2. Stop the writers (the rewrite must not race appends):
   `docker compose stop consumer`.
3. Repair. For one/few known keys, do it server-side (Athena, ~40 s per key;
   stop consumers first). Copy the row, delete **both** copies, insert one:
   ```sql
   SELECT * FROM bronze_trades WHERE symbol='BTCUSDT' AND trade_id=123;
   DELETE FROM bronze_trades WHERE symbol='BTCUSDT' AND trade_id=123;
   INSERT INTO bronze_trades (symbol, trade_id, price, quantity, event_time,
     ingest_time, is_buyer_maker) VALUES ('BTCUSDT', 123, ..., timestamp '...',
     timestamp '...', true);
   ```
   `make dedup-key KEY=BTCUSDT:123` / `make dedup-bronze DAYS=3` use PyIceberg
   instead; they are only practical on a small/compacted table (a 3-day rewrite
   of the uncompacted bronze did not finish in 15 min).
4. `make up` (`--scale consumer=3`) to restart the fixed image.
5. Re-check: `c == d`, then `make quality` (uniqueness gate green) and
   `make dbt-build`.

## Backlog drain procedure (after long outages)

1. `make up` (or `up -d kafka` if only the broker died).
2. Watch consumer: `batch_committed` rows/appended/skipped + `lag_messages`
   trending down. 20k-row batches take ~1 min; a ~1M backlog takes ~1 h.
3. Don't run `make maintenance` mid-drain (OPTIMIZE races the writer;
   harmless but wasteful — CommitFailed retries).
4. When `max(event_time)` is fresh: `make quality && make dbt-build && make superset`.
   If application code changed, rebuild that service's image first; `make superset`
   runs the baked provisioning script.
5. Verify dashboard numbers move; commit nothing until green.

## Tear down

`tofu destroy` inside `infra/` — **approval required** (AGENTS.md safety rules). Empty
the lake bucket first (`force_destroy = false` blocks destroying a non-empty bucket).
`docker compose down -v` removes local volumes (Kafka data, Superset metadata).

## Kline backfill (history for charts, ADR-026)

1. Stop the scheduler first (`docker compose stop scheduler`); dbt must not read
   the table while it is being replaced.
2. `make backfill` — defaults to the first day of the month 60 days ago through
   yesterday, for every `TICKSTREAM_SYMBOLS` entry. Narrow it with
   `START=2026-08-01 END=2026-10-23 SYMBOLS=BTCUSDT`.
3. `FORCE=1` **replaces the whole `bronze_klines` table** in one commit; files
   and SHA256 checksums are validated before anything is written. Without
   `FORCE` it skips when rows exist, so an accidental run cannot duplicate
   history — but it also cannot extend an existing table partially.
4. Then `make dbt-build` (creates `silver_klines`, `gold_candles_1m_hist`, the
   `gold_candles_1m_unified` view and runs the kline tests), followed by
   `make compact` and `docker compose start scheduler`. Verify in Athena:
   `SELECT source, count(*) FROM gold_candles_1m_unified WHERE symbol='BTCUSDT' GROUP BY 1`.
5. Failures are usually a missing daily file (T+1) or a checksum mismatch; the
   log names the URL. Today's UTC day is live-only until tomorrow's archive.
   The Oct 2026 recovery rerun loaded 846,720 rows for Aug 1–Oct 23.

## Ticker stream (`@ticker`, ADR-029)

- Services: `ticker-producer` → `ticker.raw` → `ticker-consumer` →
  `bronze_tickers`. Check `docker logs tickstream-ticker-producer` for
  `ws_connected`/`ws_throughput` (expect ~5–7 msg/s) and
  `docker logs tickstream-ticker-consumer` for `batch_committed` (~500 rows,
  ~20–25 s per commit).
- Verify: `SELECT max(event_time) FROM tickstream.bronze_tickers` should be
  within a minute; `make dbt-build` creates `silver_tickers` and
  `gold_market_microstructure_1m`. The GX gate skips the ticker suite only if
  `bronze_tickers` does not exist yet.

## After a host stop or sleep (Docker daemon down)

1. Start Docker Desktop and `make up`; the Kafka volume and consumer offsets
   survive, producers reconnect automatically (check `ws_connected`).
2. Repair history: `docker compose stop scheduler` → `make backfill FORCE=1` →
   `make dbt-build` → `docker compose start scheduler`. Defaults and replacement
   semantics are in [Kline backfill](#kline-backfill-history-for-charts-adr-026).
3. The first GX cycle can fail `expect_column_max_to_be_between` because the
   window starts at the pre-stop success and the streams were paused. It
   self-heals on the next cycle once live data arrives; do not reset offsets.
4. Trades/candles for the paused days come from the archives (T+1). Ticker
   microstructure cannot be recovered — there is no historical book archive.
5. If the bucket accumulated small files, run `make compact` during a quiet
   window; see [maintenance](#maintenance-and-compaction).
