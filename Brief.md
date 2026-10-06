# Real-Time Crypto Streaming Pipeline: Project Brief

## 1. Goal

Build an end-to-end streaming data pipeline that ingests live Binance trade data, lands it in an Apache Iceberg lake on AWS S3, models it with dbt, validates it with Great Expectations, and serves dashboards in Apache Superset. The aim is a small project that works end to end and that I can explain decision by decision.

## 2. Scope

- **Start with one coin: XRP (XRPUSDT).** Expand to six or seven popular coins (for example BTC, ETH, SOL, ADA, DOGE, plus LINK or AVAX) only after the single-coin pipeline is solid.
- **Streaming ingestion, micro-batch analytics.** Events flow continuously into Kafka and Iceberg; dbt and quality checks run on a schedule (every \~10 minutes).
- **Out of scope:** Spark, Airflow, equities, trading logic, ML. Airflow and a second exchange are optional later extras.

## 3. Architecture

Binance WebSocket -> Python producer -> Kafka -> Python consumer (PyIceberg) -> S3 bronze Iceberg table (Glue catalog) -> Great Expectations checks -> dbt on Athena (silver, gold) -> Superset

Everything except S3, Glue, and Athena runs locally in Docker Compose. AWS resources are created with OpenTofu.

## 4. Stack

| Layer | Tool | Notes |
| --- | --- | --- |
| Source | Binance WebSocket (trade stream) | Public market data, no paid plan; confirm the endpoint in current Binance docs |
| Ingestion | Python producer | Reconnect logic, one message per trade |
| Broker | Apache Kafka (KRaft, single broker) | Topic per stream, keyed by symbol |
| Landing | Python Kafka consumer using PyIceberg | Micro-batches events and appends them to an Iceberg table |
| Lake | AWS S3 with Apache Iceberg tables | Parquet data files, partitioned by date and symbol |
| Catalog | AWS Glue Data Catalog | Registers the Iceberg tables so Athena can query them |
| Warehouse | AWS Athena (default) | Redshift Serverless is the alternative if the cost is acceptable |
| Transformations | dbt (dbt-athena adapter) | Incremental models on Iceberg tables |
| Data quality | Great Expectations | Runs before each dbt build |
| Scheduling | Small container looping GE then dbt build | Replaces Airflow for the MVP |
| Infrastructure as code | OpenTofu | Creates the S3 bucket, Glue database, Athena workgroup, IAM, and budget alert; one command to tear down |
| Visualisation | Apache Superset | Connects to Athena |

**On Iceberg:** Iceberg is a table format that sits on top of S3, not an alternative to it. It adds ACID commits, schema evolution, time travel, and safe row-level updates (MERGE), which makes deduplication and corrections much cleaner than raw Parquet files.

## 5. Data model (medallion)

- **Bronze:** raw trade events as received (symbol, price, quantity, trade ID, event time, ingest time), stored as an Iceberg table.
- **Silver:** deduplicated on trade ID, typed, and timestamped in UTC.
- **Gold:** 1-minute OHLC candles, VWAP, trade counts, and rolling volatility per symbol.

## 6. Data quality (Great Expectations)

Checks run on each new batch before dbt builds:

- Schema and required columns present, no nulls in key fields
- Price and quantity greater than zero
- Trade ID uniqueness after dedup
- Freshness: latest event no older than a set threshold

Failed checks stop the dbt run and are written to a visible report.

## 7. Dashboards (Superset)

- Live-ish price and candle chart per symbol
- Volume and VWAP over time
- Volatility panel
- Pipeline health panel (data freshness, row counts, failed checks)

## 8. Phases

1. **Phase 0, infrastructure:** OpenTofu creates the S3 bucket, Glue database, Athena workgroup, least-privilege IAM user, and an AWS budget alert before anything else.
2. **Phase 1, XRP only:** WebSocket into Kafka, consumer appending to the bronze Iceberg table, verify data lands correctly and survives a restart.
3. **Phase 2:** dbt silver and gold models on Athena, Great Expectations checks, scheduled run, Iceberg maintenance (see section 9).
4. **Phase 3:** Superset dashboard, then expand to six or seven coins.
5. **Phase 4:** README polish, architecture diagram, cost breakdown, and lessons learned.
6. **Optional extras:** Airflow, a second exchange.

## 9. Engineering concerns to demonstrate

- Duplicate handling and idempotent writes (safe replays), using MERGE on Iceberg
- **Small files:** frequent micro-batch commits create many small files; schedule compaction (Athena OPTIMIZE) and snapshot cleanup (VACUUM)
- Late or out-of-order events
- Reconnect and backoff on WebSocket drops
- Replay from Kafka offsets
- Schema evolution using Iceberg
- Monitoring of consumer lag and data freshness

## 10. Cost guardrails

- Budget alert is created by OpenTofu before any other resource
- Dedicated IAM user with least-privilege access
- Partition by date and symbol and query only needed partitions in Athena
- Commit in micro-batches (for example every 30 to 60 seconds), not per event, to limit small files and request costs
- Tear down with a single OpenTofu destroy when not in use

## 11. Definition of done

- One command (`docker compose up`) starts all local services, and OpenTofu provisions AWS
- Data flows from Binance to the dashboard with no manual steps
- Quality checks and dbt tests pass in the scheduled run
- README includes an architecture diagram, setup steps, design decisions, cost summary, and "what I'd change at scale"