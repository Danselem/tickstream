# Runbook

**What this answers:** how to start the system, check that it is healthy, and
recover from the failures that have actually happened.

Each procedure follows the same shape: **preconditions → steps → verify**. Do not
skip the verify step; a command that exits 0 can still leave the pipeline wrong.

New to the terms? Use the [glossary](glossary.md). For why a failure happens, see
[architecture](architecture.md#failure-modes-seen-in-practice-and-the-fixes).

## Safety rules

- Never commit `.env` or AWS keys.
- `make infra-apply` and `tofu destroy` require explicit human approval. Always
  review `make infra-plan` first.
- Never reset Kafka offsets or delete lake data without approval.
- Stop the scheduler before replacing `bronze_klines`.
- Stop consumers before repairing bronze duplicates.
- Do not run compaction during a backlog drain or backfill; it races writers.
- Do not scale trade consumers above three. There are three partitions, and each
  symbol must have one writer.
- Plan infrastructure with the real `ALERT_EMAIL`; the placeholder default can
  replace the real budget notification address.

## Prerequisites

| Tool | Version / purpose |
|---|---|
| Python | 3.12, installed through `uv` |
| `uv` | Dependency and command runner |
| Docker Desktop | Local Kafka, consumers, scheduler, Superset |
| OpenTofu | ≥ 1.9, for AWS resources |
| AWS CLI | Credentials allowed to create the bootstrap and stack |
| `openssl` | Generate the Superset secret |

You also need an email address for the AWS budget alert.

## First-time setup

**Preconditions:** Docker is running; AWS credentials work; `.env` does not exist
yet.

1. Create the environment file.

   ```bash
   cp .env.example .env
   openssl rand -hex 32
   ```

   Put the generated value in `SUPERSET_SECRET_KEY`, choose a strong
   `SUPERSET_ADMIN_PASSWORD`, and fill in the AWS bucket name and region. Leave the
   pipeline AWS key blank until step 7: infrastructure commands use the AWS CLI
   credentials already active in your shell, not `.env`.

2. Install dependencies.

   ```bash
   make install
   ```

3. Prove the offline code path before touching AWS.

   ```bash
   make lint typecheck test
   ```

   **Expected:** Ruff and mypy pass, `118 passed, 1 deselected`.

4. Create the remote state backend once.

   ```bash
   make infra-bootstrap
   ```

5. Plan AWS with the real email.

   ```bash
   make infra-plan ALERT_EMAIL=you@example.com
   ```

   **Expected:** the plan includes the budget alert, declared before the other
   main-stack resources. Check the email; OpenTofu may create independent
   resources concurrently.
   Read the plan; do not approve it blindly.

6. Apply only with explicit approval.

   ```bash
   make infra-apply
   ```

7. Create an access key for the `tickstream-pipeline` IAM user and add it to
   `.env`. Do not commit it.

   ```bash
   aws iam create-access-key --user-name tickstream-pipeline
   ```

   Map `AccessKeyId` to `AWS_ACCESS_KEY_ID` and `SecretAccessKey` to
   `AWS_SECRET_ACCESS_KEY`. AWS shows the secret once; store it before leaving the
   page. Makefile targets that touch the lake load `.env`; `make infra-*` uses the
   AWS CLI credentials in your shell instead.

8. Start the stack.

   ```bash
   make up
   ```

   **Expected:** dashboard at <http://localhost:8088>; log in with the admin values
   from `.env`; trades and quotes appear within a minute or two; gold and Market
   charts populate after the first quality → dbt cycle (~30–35 minutes).

**Verify:** the dashboard opens, the Symbol filter defaults to BTC, and
`make health` returns rows. If charts are empty, gold has not been built yet: run
`make dbt-build` or wait for the cycle.

## Daily commands

Use `make help` for the full list.

| Command | What it does | Expected |
|---|---|---|
| `make up` | Start Kafka, producers, consumers, scheduler, Superset | Containers healthy; dashboard reachable |
| `make down` | Stop the stack without deleting volumes | Containers stopped; data remains |
| `make logs` | Follow service logs | JSON log lines with no repeating errors |
| `make quality` | Run GX once | All expectations pass; report written |
| `make dbt-build` | Build and test all dbt models; this target does not run GX | 128 nodes pass |
| `make integrity` | Whole-table duplicate check | `duplicates = 0` |
| `make health` | Row counts and freshness snapshot | New rows in `layer_counts` and `symbol_freshness` |
| `make maintenance` | `OPTIMIZE` + `VACUUM` the five core tables | Completed or a retryable commit error |
| `make compact TABLES=...` | Compact a narrower table list | File count falls |
| `make superset` | Re-run dashboard provisioning | Chart count stable; no duplicates |
| `make test` | Offline unit tests | 118 passed, 1 deselected |

**Images are baked, not mounted.** A code change needs a rebuild:

```bash
docker compose build <service>
docker compose up -d --no-deps --force-recreate <service>
```

Each `build: .` service has its own image. Rebuilding `consumer` does **not** update
`ticker-consumer` (`tickstream-ticker-consumer`). Scaled trade consumers are
`tickstream-consumer-1/2/3`; read them with `docker logs tickstream-consumer-1`.
The form `docker compose logs consumer-1` does not exist.

## Dashboards

- URL: <http://localhost:8088>; dashboard `tickstream` (published).
- **Market** tab: candlestick, price/VWAP, base volume, USD notional, trades/min,
  volatility, quoted spread, book imbalance, notional treemap, two headline-number
  tiles, and daily/hourly summaries.
- **Pipeline health** tab: lag, batch rows, quality runs, bronze integrity, rows per
  layer, and per-symbol freshness.
- Datasets: 10 total. They read gold and ops/health tables, never raw bronze.
- **Symbol** filter: defaults to BTC and scopes the Market charts.
- **Time range** filter: applies to the dashboard.
- XRP remains queryable in old data but is not in the configured symbol list.

After changing `TICKSTREAM_SYMBOLS` or `provision.py`, rebuild Superset and run
`make superset`. The target runs the script baked into the image, not the host
file. If charts are empty after boot, wait for the first GX → dbt cycle, or run
`make quality && make dbt-build` for a manual build.

## Incident triage

Start with the symptom, then run the first check. Do not “fix” by resetting
offsets.

| Symptom | First check | Fix |
|---|---|---|
| Consumer restarts every few minutes; `curlCode: 28`; lag and rebalances | `docker compose logs consumer`; count retries | Transient-only retry, longer S3 timeouts, 10-minute poll interval |
| `batch_committed` shows `rows=1`; offset moves by 1; `max_event_time` frozen | `docker logs tickstream-consumer-1`; look for `batch_committed` | Restart a stuck replica; otherwise check `_flush()` ordering (ADR-014/030). Reset the window only at the end of the flush |
| Lag is uneven or not draining | `docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group bronze-writer` | Symbols are pinned: BTC→1, ETH/DOGE/LINK→2, SOL/ADA/AVAX→0. Rebalance the map if the mix changed. Never scale above 3 |
| Kafka broker died (exit 137, host outage) | `docker compose up -d kafka`; check the `kafka-data` volume | Backlog drains at ~20k/batch; freshness heals once fresh timestamps commit |
| dbt Glue timeout on MERGE | Check the affected model and its lookback | `update_condition` skips identical rows (ADR-009); suspect a large lookback |
| GX failing | `docker logs tickstream-scheduler`; latest manual `quality/reports/*.json`; `quality_results.failed_expectations` | Freshness heals after fresh data; duplicates use the dedup procedure. A failed run does not advance the window |
| Superset 500 on dataset sync | `docker logs tickstream-superset` and the traceback | Usually IAM (`GetTableMetadata`/`ListDatabases`) or catalog case (ADR-012) |
| Superset login fails after rebuild | Check the `superset-home` volume | Admin persists on the volume; if deleted, bootstrap recreates it from `.env` |
| Code change has no effect | Check which image the service runs | Baked images, not host mounts. Rebuild the exact service; `ticker-consumer` is separate |
| Charts empty after fresh boot | Check whether gold tables exist | Wait for GX → dbt, or run `make dbt-build` then `make superset` |

## Backlog drain

**Preconditions:** Kafka is running; no maintenance or backfill is in progress;
the service image matches the current code.

1. Start the stack, or just the broker if only Kafka died.

   ```bash
   make up
   # or:
   docker compose up -d kafka
   ```

2. Watch the consumer throughput and lag.

   ```bash
   docker logs -f tickstream-consumer-1
   ```

   **Expected:** `batch_committed` shows `rows`, `lag_messages`, and `commit_ms`;
   `bronze_write` shows `appended` and `skipped`. `lag_messages` trends down. A
   20k-row batch takes about a minute; a ~1M backlog takes about an hour.

3. Do not run `make maintenance` while draining. `OPTIMIZE` races the writer; it is
   usually harmless but wasteful and can produce commit retries.

4. When `max(event_time)` is fresh, run the normal chain.

   ```bash
   make quality && make dbt-build && make superset
   ```

5. **Verify:** dashboard values move, lag is near zero, and no `rows: 1` appears in
   `batch_committed`.

## Bronze dedup repair

**Preconditions:** `make integrity` reports `duplicates > 0`; you know the affected
key or accept a table rewrite; consumers can be stopped.

The scheduled integrity check is the detector. The recent GX window can miss an
old duplicate.

1. Confirm the damage. `0` is expected for duplicates.

   ```sql
   SELECT count(*) AS c,
          count(DISTINCT symbol || '#' || CAST(trade_id AS varchar)) AS d
   FROM tickstream.bronze_trades;
   ```

2. Stop the writers.

   ```bash
   docker compose stop consumer
   ```

3. Repair one or a few known keys in Athena. Read the row, delete both copies,
   insert one. This takes about 40 seconds per key and prunes by partition.

   ```sql
   SELECT * FROM tickstream.bronze_trades
   WHERE symbol = 'BTCUSDT' AND trade_id = 123;

   DELETE FROM tickstream.bronze_trades
   WHERE symbol = 'BTCUSDT' AND trade_id = 123;

   INSERT INTO tickstream.bronze_trades
     (symbol, trade_id, price, quantity, event_time, ingest_time, is_buyer_maker)
   VALUES
     ('BTCUSDT', 123, ..., timestamp '...', timestamp '...', true);
   ```

   For a small or compacted table, `make dedup-bronze KEY=BTCUSDT:123` and
   `make dedup-bronze DAYS=3` use PyIceberg instead. A three-day rewrite of the
   uncompacted table did not finish in 15 minutes, so prefer Athena for a large
   table.

4. Restart the consumers.

   ```bash
   make up
   ```

5. **Verify:** the duplicate query shows `c == d`; `make quality` passes;
   `make dbt-build` passes.

This is the only sanctioned mutation of `bronze_trades`. The separate kline
backfill replaces `bronze_klines` in one commit ([ADR-026](decisions.md#adr-026--archive-1m-klines-backfill-for-chart-history-bronze_klines)).
The repair restores the no-duplicate invariant the quality gate depends on.

## Kline backfill (history for charts, ADR-026)

**Preconditions:** the scheduler is stopped; AWS credentials are loaded; enough
local disk for the zip files.

1. Stop the scheduler. dbt must not read the table while it is replaced.

   ```bash
   docker compose stop scheduler
   ```

2. Load the archive. Defaults: first day of the month 60 days ago through
   yesterday, all configured symbols.

   ```bash
   make backfill
   ```

   Narrow it if needed:

   ```bash
   START=2026-08-01 END=2026-10-23 SYMBOLS=BTCUSDT make backfill
   ```

3. Understand the replacement rule:

   - `FORCE=1` replaces the **whole** `bronze_klines` table in one commit.
   - Without `FORCE`, the run skips when rows exist, so it cannot duplicate
     history, but it also cannot extend an existing table partially.
   - Files and SHA256 sidecars are checked before anything is written.

4. Build the models and compact.

   ```bash
   make dbt-build
   make compact TABLES=bronze_klines
   docker compose start scheduler
   ```

5. **Verify** the sources in Athena:

   ```sql
   SELECT source, count(*)
   FROM tickstream.gold_candles_1m_unified
   WHERE symbol = 'BTCUSDT'
   GROUP BY 1;
   ```

   **Expected:** both `live` and `kline` rows. The October 2026 rerun loaded
   **846,720 rows** for 1 Aug–23 Oct.

Failures are usually a missing daily file (T+1) or a checksum mismatch; the log
names the URL. Today's UTC day is live-only until tomorrow's archive.

## Host recovery after stop or sleep

**Preconditions:** Docker Desktop is running again; the Kafka volume still exists.

1. Start the stack.

   ```bash
   make up
   ```

   The Kafka volume and consumer offsets survive. Producers reconnect
   automatically. Check for `ws_connected`.

2. Repair candle history.

   ```bash
   docker compose stop scheduler
   make backfill FORCE=1
   make dbt-build
   docker compose start scheduler
   ```

3. The first GX cycle may fail on freshness because the quality window starts at
   the pre-stop success and the streams were paused. It self-heals on the next
   cycle after live data arrives. **Do not reset offsets.**

4. **Verify:** `make health` shows fresh timestamps; `quality_results` is green;
   the dashboard moves.

Kafka replays messages already accepted before the pause, if they are still
within the seven-day retention. Trades and quotes from the paused period were
never produced and are lost; there is no trade archive. Candles rebuild from the
T+1 kline archive. Quote microstructure also has no archive.

If the lake accumulated small files, run `make compact` during a quiet window.
The default table is `bronze_trades`; pass `TABLES=` to choose another table.

## Add a symbol

**Preconditions:** the symbol exists on Binance Spot and is quoted in USDT; the
partition map is balanced.

1. Add the uppercase symbol to `TICKSTREAM_SYMBOLS` in `.env`.

   ```text
   TICKSTREAM_SYMBOLS='["BTCUSDT","ETHUSDT","SOLUSDT","ADAUSDT","DOGEUSDT","LINKUSDT","AVAXUSDT","BNBUSDT"]'
   ```

2. Map it to a partition in `TICKSTREAM_SYMBOL_PARTITIONS`. Keep the three
   partitions balanced. For example, add `BNBUSDT` to partition 1. Unmapped symbols
   fall back to the hash.

3. Restart with the new environment.

   ```bash
   make restart
   ```

4. **Verify:** `ws_connected` and `batch_committed` appear for both the trade and
   ticker streams.

5. If `provision.py` changed, rebuild Superset and run `make superset`; the Symbol
   filter picks up configured coins.

6. For history, stop the scheduler, replace the kline table, rebuild, and restart:

   ```bash
   docker compose stop scheduler
   make backfill FORCE=1
   make dbt-build
   docker compose start scheduler
   ```

   The archive is T+1 and has no order-book data.

## Rotate secrets

**Superset secret**

- Replacing `SUPERSET_SECRET_KEY` invalidates existing sessions.
- Recreate the container, then log in again:

  ```bash
  docker compose up -d --no-deps --force-recreate superset
  ```

**AWS access keys**

1. Create a new key for `tickstream-pipeline`.
2. Update `.env`.
3. Recreate the Python services.
4. **Verify** with `make health`.
5. Deactivate the old key.

`.env.example` contains placeholders only.

## Maintenance and compaction

- The scheduler runs `OPTIMIZE … REWRITE DATA USING BIN_PACK` and `VACUUM` every
  72 cycles on `bronze_trades`, `silver_trades`, `gold_candles_1m`,
  `gold_market_summary_daily`, and `gold_market_summary_hourly`. (~1.5–2 days at the current cycle time).
- `make maintenance` runs the same work now; `make compact TABLES=...` narrows it.
  `make compact` bypasses the quality gate and only touches the tables you pass.
- Do not run during a backlog drain or backfill. It races writers and wastes
  requests.
- A maintenance statement can fail with `ICEBERG_COMMIT_ERROR` when consumers
  append while maintenance commits. `VACUUM` may still succeed and no data is lost;
  retry in a quiet window. Tickers, klines, and ops tables are not in the scheduled
  list; compact them explicitly when needed.
- After a large load, compact, then check object counts in S3. Small files, not
  bytes, drive the request bill.

**Verify:** file count falls; `make integrity` still returns zero duplicates; the
dashboard still reads.

## Ticker stream

**Preconditions:** `ticker-producer` and `ticker-consumer` are running.

1. Check the producer.

   ```bash
   docker logs tickstream-ticker-producer
   ```

   **Expected:** `ws_connected` and `ws_throughput` at ~5–7 messages/s.

2. Check the consumer.

   ```bash
   docker logs tickstream-ticker-consumer
   ```

   **Expected:** `batch_committed` at full-size batches roughly every 45 seconds,
   not `rows=1`.

3. **Verify** freshness:

   ```sql
   SELECT max(event_time) FROM tickstream.bronze_tickers;
   ```

   **Expected:** within a minute of now.

`make dbt-build` creates `silver_tickers` and
`gold_market_microstructure_1m`. The GX gate skips the ticker suite only if
`bronze_tickers` does not exist yet.

## Teardown

**This deletes data.** Get explicit approval first.

1. Empty the lake bucket. `force_destroy = false` blocks destroying a non-empty
   bucket.
2. From `infra/`, run `tofu destroy`.
3. Remove local volumes:

   ```bash
   docker compose down -v
   ```

This deletes Kafka data and Superset metadata. It cannot be undone. The bootstrap
state bucket and lock table are separate from the main stack and are not removed by
this command.
