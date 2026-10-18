"""Bronze writer tests against an offline SQL catalog (no AWS, no Kafka).

The replay test is the guardrail for the known PyIceberg partitioned-upsert
regression (issue #3758): writing the same batch twice must not duplicate rows.
"""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError

from pipeline.common.config import Settings
from pipeline.common.iceberg import (
    BRONZE_ARROW_SCHEMA,
    BRONZE_TABLE,
    ensure_bronze_table,
    events_to_arrow,
    load_lake_catalog,
)
from pipeline.common.models import TradeEvent
from pipeline.consumer.bronze_writer import BronzeWriter

NAMESPACE = "testdb"


def _settings() -> Settings:
    return Settings(catalog_type="sql", glue_database=NAMESPACE)


def _event(trade_id: int) -> TradeEvent:
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    return TradeEvent(
        symbol="XRPUSDT",
        trade_id=trade_id,
        price=Decimal("0.52130"),
        quantity=Decimal("150.5"),
        event_time=now,
        ingest_time=now,
        is_buyer_maker=False,
    )


def _catalog(tmp_path: Path) -> tuple[Catalog, Settings]:
    settings = _settings()
    catalog = load_lake_catalog(settings, tmp_path)
    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass
    return catalog, settings


def test_ensure_bronze_table_is_idempotent(tmp_path: Path) -> None:
    catalog, settings = _catalog(tmp_path)
    first = ensure_bronze_table(catalog, settings.glue_database)
    second = ensure_bronze_table(catalog, settings.glue_database)
    assert first.metadata.location == second.metadata.location


def test_arrow_conversion_matches_bronze_schema() -> None:
    arrow = events_to_arrow([_event(1), _event(2)])
    assert arrow.num_rows == 2
    assert arrow.schema == BRONZE_ARROW_SCHEMA


def test_replay_does_not_duplicate_rows(tmp_path: Path) -> None:
    catalog, settings = _catalog(tmp_path)
    writer = BronzeWriter(settings, tmp_path)
    events = [_event(trade_id) for trade_id in range(5)]
    assert writer.write(events) == 5
    assert writer.write(events) == 0  # replay appends nothing (no empty commit)
    table = catalog.load_table(f"{NAMESPACE}.{BRONZE_TABLE}")
    rows = table.scan().to_arrow()
    assert rows.num_rows == 5
    assert sorted(r["trade_id"] for r in rows.to_pylist()) == [0, 1, 2, 3, 4]


def test_partial_overlap_appends_only_new_rows(tmp_path: Path) -> None:
    catalog, settings = _catalog(tmp_path)
    writer = BronzeWriter(settings, tmp_path)
    assert writer.write([_event(trade_id) for trade_id in range(5)]) == 5
    assert writer.write([_event(trade_id) for trade_id in range(3, 8)]) == 3
    table = catalog.load_table(f"{NAMESPACE}.{BRONZE_TABLE}")
    assert table.scan().to_arrow().num_rows == 8


def test_within_batch_duplicates_written_once(tmp_path: Path) -> None:
    catalog, settings = _catalog(tmp_path)
    writer = BronzeWriter(settings, tmp_path)
    events = [_event(1), _event(2), _event(1)]  # same key twice in one batch
    assert writer.write(events) == 2
    table = catalog.load_table(f"{NAMESPACE}.{BRONZE_TABLE}")
    assert table.scan().to_arrow().num_rows == 2


def test_writer_rejects_empty_batch(tmp_path: Path) -> None:
    _, settings = _catalog(tmp_path)
    writer = BronzeWriter(settings, tmp_path)
    with pytest.raises(ValueError, match="empty batch"):
        writer.write([])
