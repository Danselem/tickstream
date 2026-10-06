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

    # Kafka
    kafka_bootstrap: str = "localhost:9092"
    topic: str = "trades.raw"
    consumer_group: str = "bronze-writer"

    # Source
    symbols: list[str] = ["XRPUSDT"]
    binance_ws_url: str = "wss://data-stream.binance.vision"

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
