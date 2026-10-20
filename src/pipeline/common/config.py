"""Central configuration. Env vars only — see .env.example. Never commit secrets."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All runtime configuration, loaded from `TICKSTREAM_*` env vars / `.env`."""

    model_config = SettingsConfigDict(env_prefix="TICKSTREAM_", env_file=".env", extra="ignore")

    # AWS / lake
    aws_region: str = "us-east-1"
    s3_bucket: str = "tickstream-lake"
    glue_database: str = "tickstream"
    catalog_type: str = "glue"  # "glue" in prod, "sql" for offline pytest
    # PyIceberg S3 timeouts (s). Defaults raised after Oct 2026 incident: the
    # stock ~3 s timeouts turned Docker-network degradation into a crash loop.
    s3_connect_timeout_s: float = 15.0
    s3_request_timeout_s: float = 60.0

    # Kafka
    kafka_bootstrap: str = "localhost:9092"
    topic: str = "trades.raw"
    consumer_group: str = "bronze-writer"
    ticker_topic: str = "ticker.raw"
    ticker_consumer_group: str = "ticker-writer"

    # Source
    symbols: list[str] = ["XRPUSDT"]
    # Optional explicit Kafka partition per symbol. Unmapped symbols fall back to
    # the key hash; pinning a few hot symbols keeps one partition from owning
    # most of the traffic (ADR-028). Values must be valid partition indexes.
    symbol_partitions: dict[str, int] = {}
    binance_ws_url: str = "wss://data-stream.binance.vision"
    # Websocket keepalive (s). ping_timeout raised after Oct 2026: the 20 s default
    # tripped "keepalive ping timeout" (close 1011) on Docker-network hiccups.
    ws_ping_interval_s: int = 20
    ws_ping_timeout_s: int = 60
    # Reconnect when the socket stays open but delivers nothing. Binance has
    # left combined streams silent for hours while keepalive pings still ponged.
    ws_stale_after_s: int = 60

    # Batching / scheduling
    batch_interval_s: int = 45
    batch_max_rows: int = 20_000
    freshness_max_lag_s: int = 300
    quality_interval_s: int = 600
    maintenance_every_n_runs: int = 72

    log_level: str = "INFO"


def get_settings() -> Settings:
    """Build Settings from the environment (call once per process entrypoint)."""
    return Settings()
