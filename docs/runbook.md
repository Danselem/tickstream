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
`make dbt-build` / `make maintenance`; `make superset` re-runs dashboard
provisioning against the live container (idempotent).

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
  A failed run does NOT advance the scan window.
- **Superset 500s on dataset sync.** Almost always IAM (`athena:GetTableMetadata`
  / `ListDatabases` datacatalog-scoped) or `catalog_name` case — see ADR-012.
  Server traceback: `docker logs tickstream-superset | grep -A15 datasets/api.py`.
- **Superset login fails after rebuild.** Metadata DB lives on the
  `superset-home` volume; admin persists. If the volume was deleted, bootstrap
  recreates the admin from `.env`.

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
