"""Unit tests for central configuration loading (offline)."""

import os

from _pytest.monkeypatch import MonkeyPatch

from pipeline.common.config import Settings, get_settings
from pipeline.common.iceberg import load_lake_catalog


def test_env_overrides_use_tickstream_prefix(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("TICKSTREAM_SYMBOLS", '["BTCUSDT","ETHUSDT"]')
    monkeypatch.setenv("TICKSTREAM_BATCH_INTERVAL_S", "30")
    settings = get_settings()
    assert settings.symbols == ["BTCUSDT", "ETHUSDT"]
    assert settings.batch_interval_s == 30


def test_defaults_when_no_env_and_no_dotenv(monkeypatch: MonkeyPatch, tmp_path: object) -> None:
    monkeypatch.chdir(tmp_path)  # type: ignore[arg-type]
    for var in [v for v in os.environ if v.startswith("TICKSTREAM_")]:
        monkeypatch.delenv(var, raising=False)
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.symbols == ["XRPUSDT"]
    assert settings.topic == "trades.raw"
    assert settings.catalog_type == "glue"
    assert settings.s3_connect_timeout_s == 15.0
    assert settings.s3_request_timeout_s == 60.0


def test_glue_catalog_carries_s3_timeouts() -> None:
    settings = Settings(catalog_type="glue", s3_connect_timeout_s=5.0)
    catalog = load_lake_catalog(settings)  # construct only; no network
    assert catalog.properties["s3.connect-timeout"] == "5.0"
    assert catalog.properties["s3.request-timeout"] == "60.0"
