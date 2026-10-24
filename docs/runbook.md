# Runbook

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
`make dbt-build` / `make maintenance`; `make integrity` (whole-table duplicate
check; also runs automatically on maintenance cycles); `make superset` re-runs
dashboard provisioning against the live container (idempotent).

## Dashboards

- URL http://localhost:8088 → dashboard `tickstream` (published).
- Charts: price/volume/volatility per symbol (groupby), consumer lag + batch
  rows, recent quality runs. Datasets: `gold_candles_1m`, `quality_results`,
  `pipeline_metrics` (never raw bronze).
- After changing `TICKSTREAM_SYMBOLS`, run `make superset` to refresh charts.

## Incidents and how they were fixed

- **Consumer crash loop (S3 timeouts).** Symptom: container restarts every few
  minutes, `OSError curlCode: 28` in logs, Kafka lag explodes, rebalances.
  Cause: short pyarrow S3 timeouts vs degraded Docker networking. Fix: transient
  retry (`common/retry.py`), raised timeouts, `max.poll.interval.ms` 10 min.
  Check: `docker compose logs consumer | grep -c Retrying`.
- **Consumer stall (1-row batches).** Symptom: `batch_committed` shows `rows=1`
  every ~40 s, the committed offset advances by 1, `max_event_time` frozen,
  lag grows. Two causes seen: (a) a librdkafka stuck state after violent restart
  churn — cured by `docker compose restart consumer`; (b) the batching-window
  bug fixed in ADR-014. If it recurs, restart the replica(s) and check
  `make lint typecheck test` still covers the batcher regression.
  Check: `docker compose logs consumer | grep batch_committed | tail`.
- **Lag not draining / uneven.** Each replica owns one partition; `trades.raw`
  partition 0 carried the historical XRP backlog and drains slower. Watch
  per-partition lag: `docker compose exec kafka /opt/kafka/bin/\
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe \
  --group bronze-writer`. `make up` runs 3 consumer replicas (`--scale
  consumer=3`); never scale beyond the partition count (3).
- **Kafka broker died (exit 137, host outage).** Data survives on the
  `kafka-data` volume; `docker compose up -d kafka` recovers. ~1M backlog then
  drains at ~20k/batch — watch `lag_messages` in consumer logs. Freshness gate
  stays red until fresh timestamps commit (expected; self-heals).
- **dbt Glue timeout on MERGE.** `update_condition` skips identical rows
  (ADR-009). If a MERGE ever times out again, suspect lookback size first.
- **GX failing.** Read `quality/reports/<latest>.json`: `failed_expectations`
  names the check. Freshness failures after downtime heal once fresh data lands.
  A failed run does NOT advance the scan window. If it's
  `expect_column_values_to_be_unique` on bronze `unique_key`, see the dedup
  procedure below (cross-replica duplication, ADR-015).
- **Superset 500s on dataset sync.** Almost always IAM (`athena:GetTableMetadata`
  / `ListDatabases` datacatalog-scoped) or `catalog_name` case — see ADR-012.
  Server traceback: `docker logs tickstream-superset | grep -A15 datasets/api.py`.
- **Superset login fails after rebuild.** Metadata DB lives on the
  `superset-home` volume; admin persists. If the volume was deleted, bootstrap
  recreates the admin from `.env`.

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
   of the current 4k-file bronze did not finish in 15 min).
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
5. Verify dashboard numbers move; commit nothing until green.

## Tear down

`tofu destroy` inside `infra/` — **approval required** (AGENTS.md safety rules). Empty
the lake bucket first (`force_destroy = false` blocks destroying a non-empty bucket).
`docker compose down -v` removes local volumes (Kafka data, Superset metadata).

## Kline backfill (history for charts, ADR-026)

1. `make backfill` — defaults to the first day of the month 60 days ago through
   yesterday, for every `TICKSTREAM_SYMBOLS` entry. Narrow it with
   `START=2026-08-01 END=2026-10-19 SYMBOLS=BTCUSDT`.
2. Re-running replaces the whole table only with `FORCE=1` (otherwise it skips
   when rows exist, so an accidental run cannot duplicate history).
3. Then `make dbt-build` (creates `silver_klines`, `gold_candles_1m_hist`, the
   `gold_candles_1m_unified` view and runs the kline tests) and
   `make superset` (dataset/charts). Verify in Athena:
   `SELECT source, count(*) FROM gold_candles_1m_unified WHERE symbol='BTCUSDT' GROUP BY 1`.
4. Failures are usually a missing daily file (T+1) or a checksum mismatch; the
   log names the URL. Today's UTC day is live-only until tomorrow's archive.

## Ticker stream (`@ticker`, ADR-029)

- Services: `ticker-producer` → `ticker.raw` → `ticker-consumer` →
  `bronze_tickers`. Check `docker logs tickstream-ticker-producer` for
  `ws_connected`/`ws_throughput` (expect ~7 msg/s) and
  `docker logs tickstream-ticker-consumer` for `batch_committed`.
- Verify: `SELECT max(event_time) FROM tickstream.bronze_tickers` should be
  within a minute; `make dbt-build` creates `silver_tickers` and
  `gold_market_microstructure_1m`. The GX gate skips the ticker suite only if
  `bronze_tickers` does not exist yet.

## After a host stop or sleep (Docker daemon down)

1. Start Docker Desktop and `make up`; the Kafka volume and consumer offsets
   survive, producers reconnect automatically (check `ws_connected`).
2. Stop the scheduler while repairing history:
   `docker compose stop scheduler` → `make backfill FORCE=1` (defaults to the
   first of the month 60 days ago through *yesterday*) → `make dbt-build` →
   `docker compose start scheduler`.
   `--force` replaces the whole `bronze_klines` table in one commit; files are
   downloaded and checksum-verified before anything is written.
3. The first GX cycle can fail `expect_column_max_to_be_between` because the
   window starts at the pre-stop success and the streams were paused. It
   self-heals on the next cycle once live data arrives; do not reset offsets.
4. Trades/candles for the paused days come from the archives (T+1). Ticker
   microstructure cannot be recovered — there is no historical book archive.
5. If the bucket accumulated small files, run `make compact` (all medallion
   tables) during a quiet window. `OPTIMIZE pipeline_metrics` can fail with
   `ICEBERG_COMMIT_ERROR` while consumers are committing; retry later — its
   `VACUUM` still succeeds and no data is affected.
