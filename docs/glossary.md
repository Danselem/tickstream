# Glossary

**What this answers:** what the technical words in this project mean, in plain
English, and why each one matters here. Terms are grouped by the part of the
pipeline where they first appear.

**How to read the docs:** start with the [README](../README.md), then the
[architecture](architecture.md). Use the [data model](data-model.md) for tables,
the [runbook](runbook.md) for operations, and the [ADRs](decisions.md) for why a
choice was made. You do not need to read this glossary front to back.

## Data and time

| Term | Plain meaning | Why it matters here |
|---|---|---|
| **Event time** | When Binance says the trade or quote happened. | Used for candles, partitions, freshness, and late-arrival handling. |
| **Ingest time** | When our consumer or producer received the event. | Shows pipeline delay; it is kept alongside event time, never instead of it. |
| **UTC** | A single world time zone with no daylight-saving changes. | Every stored timestamp is UTC so data from different machines lines up. |
| **Freshness** | How old the newest row is right now (`now − max(event_time)`). | A green dashboard with stale data is a failure; freshness is checked and charted. |
| **Backfill** | Loading older history after the live pipeline has started. | Binance kline archives restore candle history; they cannot restore trades or quotes. |
| **T+1** | Available one day after the period ends. | Binance's daily kline archive for today appears tomorrow. |

## Streaming

| Term | Plain meaning | Why it matters here |
|---|---|---|
| **WebSocket** | A long-lived connection where the server pushes messages to the client. | Binance sends trades and quotes over it instead of waiting for requests. |
| **Kafka** | A durable message queue that stores ordered streams and lets consumers replay them. | Holds trades and quotes so a consumer crash does not lose data. |
| **Topic** | A named stream of messages in Kafka, such as `trades.raw`. | Separates the trade stream from the quote (`ticker`) stream. |
| **Partition (Kafka)** | An ordered slice of a topic. Messages in one partition keep their order. | A symbol is sent to one Kafka partition, so one consumer owns it and order is preserved. |
| **Offset** | A consumer's bookmark: the next message position in a partition. | Committing it too early loses data; committing after the write makes a crash replay instead. |
| **Consumer group** | Consumers that share partitions and coordinate who reads what. | Three trade consumers split the work without reading the same partition twice. |
| **Lag** | Messages waiting to be processed (latest offset minus committed offset). | The main early warning that the consumer is falling behind. |
| **Retention** | How long Kafka keeps messages. | Seven days allows a replay after a short outage. |
| **Micro-batch** | Group many events and write them once, rather than one write per event. | Limits S3 requests and small files: 45 seconds or 20,000 rows, whichever comes first. |
| **Anti-join** | Keep only rows whose key is not already present in the target. | Bronze uses it to append new `(symbol, trade_id)` rows and skip replays. |
| **Lookback** | A fixed amount of recent time a model re-reads to catch late data. | Silver re-reads 2 hours; gold re-reads 30 minutes by default. |
| **At-least-once** | A message may be delivered more than once, but should not be lost. | Replays are expected, so every write must tolerate seeing the same event again. |
| **Idempotent** | Doing the same operation twice has the same end result as doing it once. | A replayed trade must not create a second row. |
| **KRaft** | Kafka's built-in metadata mode; it does not need a separate ZooKeeper service. | Keeps the local single-broker setup small. |
| **Replication factor (RF)** | How many copies of each partition exist. | This project uses RF=1, so one broker loss stops the local pipeline; that is an accepted MVP trade-off. |

## Lakehouse and files

| Term | Plain meaning | Why it matters here |
|---|---|---|
| **Data lake** | Cheap object storage (S3) holding files in open formats. | The source of truth for all historical data. |
| **Lakehouse** | A data lake with table features such as schemas, safe commits, and updates. | Iceberg on S3 is the lakehouse used here. |
| **PyIceberg** | The Python library that reads and writes Iceberg tables. | The consumer uses it to write bronze without a separate query engine. |
| **Table format (Iceberg)** | A layer that gives a folder of files table behaviour: schemas, snapshots, safe commits. | Lets us update, deduplicate, and query S3 data reliably. |
| **Snapshot** | One committed version of an Iceberg table. | Readers see a consistent version; old snapshots are cleaned up by `VACUUM`. |
| **Manifest** | Iceberg's list of which data files belong to a snapshot. | Lets a query skip files that cannot contain the requested day or symbol. |
| **Partition (Iceberg)** | A folder-level split of a table. Live tables use event day and symbol; archive klines use symbol only. Different from a Kafka partition. | Queries and checks read less data, which keeps Athena cheap. |
| **Partition pruning** | Skipping partitions that do not match the filter. | The difference between scanning a day and scanning the whole lake. |
| **Parquet** | A compressed column-oriented file format. | Efficient for analytics because a query reads only the columns it needs. |
| **Arrow** | An in-memory columnar format used to move data between Python tools. | Avoids slow row-by-row conversion during writes. |
| **MERGE / upsert** | Update rows whose key already exists and insert rows whose key does not. | The core of replay-safe writes; the key for trades is `(symbol, trade_id)`. |
| **Compaction** | Merge many small files into fewer larger ones. | Frequent micro-batches create small files, which make S3 requests expensive. |
| **`OPTIMIZE`** | Athena's command that compacts Iceberg data files. | Scheduled by the pipeline to control file count. |
| **`VACUUM`** | Remove old Iceberg snapshots and files that are no longer needed. | Controls storage and file-list growth without deleting current data. |
| **Bronze / silver / gold** | Three refinement levels: raw, cleaned, and business-ready. | Each layer has one job, so a bug or replay can be contained. |

## Transformation and quality

| Term | Plain meaning | Why it matters here |
|---|---|---|
| **dbt** | A tool that builds SQL models, tests them, and documents model columns. | Turns bronze data into typed silver tables and minute-level gold tables. |
| **Model** | A SQL query saved as a table or view with dependencies managed by dbt. | `stg_trades` → `silver_trades` → `gold_candles_1m` is a chain of models. |
| **Incremental model** | A model that processes only a recent window and merges the result. | Keeps scheduled builds fast instead of rescanning all history. |
| **Data test** | A rule dbt checks after building a model, such as “keys are unique”. | A failing test stops the build and prevents bad data reaching charts. |
| **Great Expectations (GX)** | A data-quality library that runs named checks against a dataset. | The gate that must pass before each scheduled dbt run; a failure blocks the cycle. |
| **Expectation / suite / checkpoint** | One check / a group of checks / one recorded run of that group. | The suite defines “good data”; the checkpoint produces pass/fail evidence. |
| **Quality gate** | A check that blocks later work when it fails. | Prevents stale or malformed bronze data from becoming gold data. |
| **Whole-table integrity check** | A slower check over every bronze key, not just the recent window. | Catches a duplicate that the recent-window quality gate could miss. |

## Warehouse and serving

| Term | Plain meaning | Why it matters here |
|---|---|---|
| **Amazon S3** | AWS object storage. | Holds the Iceberg data files and Athena query results. |
| **AWS Glue Data Catalog** | AWS's table catalogue: names, schemas, and file locations. | Athena and Superset need it to find the tables. |
| **Amazon Athena** | A serverless SQL engine that queries files in S3. | Runs dbt models and dashboard queries; billed by data scanned. |
| **Apache Superset** | A dashboard and charting tool. | Serves the Market and Pipeline health views. |
| **IAM** | AWS identity and permission system. | The pipeline gets only the S3, Glue, and Athena actions it needs. |
| **OpenTofu** | An open-source infrastructure-as-code tool; a Terraform-compatible fork. | Creates the AWS resources from committed configuration. |

## Market data

| Term | Plain meaning | Why it matters here |
|---|---|---|
| **OHLC** | Open, high, low, close: four prices for a time window. | The standard shape of a candlestick chart. |
| **Kline** | Binance's name for a candle row in its downloadable archive. | Used to rebuild chart history that the live stream missed. |
| **Candle** | One time bucket's OHLC plus volume and trade count. | The main gold table and the candlestick series. |
| **VWAP** | Volume-weighted average price: total traded value ÷ total quantity. | A price average that reflects where most volume actually traded. |
| **Notional** | Trade value in quote currency: `price × quantity` (USD for USDT pairs). | Makes volume comparable across coins with very different prices. |
| **Volatility** | How much the price moved within the minute; here the standard deviation of per-trade log returns. | A simple risk signal, not a forecast. |
| **Spread** | Best ask minus best bid. | A measure of trading cost; quoted in basis points (bps). |
| **Basis point (bps)** | One hundredth of one percent (0.01%). | Keeps small spreads readable without many decimal places. |
| **Order-book depth** | Quantity available at the best bid and ask. | Shows liquidity near the current price, not just the last trade. |
| **Imbalance** | Bid size versus ask size, scaled to `[-1, 1]`. | Positive means more displayed buying interest, negative more selling. |
| **Top of book** | The best bid and best ask only, not the full order book. | The ticker stream provides this; deeper book data is out of scope. |
| **Ticker** | Binance's 1 Hz snapshot stream of best bid/ask, last trade, and rolling 24 h stats. | Stored in `ticker.raw` / `bronze_tickers` for microstructure. |

## Operations

| Term | Plain meaning | Why it matters here |
|---|---|---|
| **ADR** | Architecture Decision Record: a short note explaining one design choice. | Records why the pipeline looks the way it does, including trade-offs. |
| **Scheduler** | The small loop that runs quality checks, dbt, and maintenance. | Replaces Airflow for one predictable workflow. |
| **Maintenance cycle** | The occasional `OPTIMIZE` + `VACUUM` + integrity + health run. | Keeps files, checks, and metadata under control every 72 cycles; it covers the five core tables, not every table. |
| **Split brain / stale handle** | Two workers act on an old view of the same table. | Caused the 15.8k duplicate incident; fixed by refreshing before the dedup scan. |
| **Single point of failure (SPOF)** | One component whose failure stops the whole system. | The local single Kafka broker is the main example. |
| **Least privilege** | Give an identity only the permissions it needs. | Limits the blast radius of a leaked key. |
| **Runbook** | Step-by-step operating instructions with checks. | Turns incident knowledge into a repeatable procedure. |
