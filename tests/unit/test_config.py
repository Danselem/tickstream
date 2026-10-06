"""Unit tests for central configuration loading (offline)."""

import os

from _pytest.monkeypatch import MonkeyPatch

from pipeline.common.config import Settings, get_settings


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
