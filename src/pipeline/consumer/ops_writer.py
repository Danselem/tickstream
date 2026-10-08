"""Ops metrics writer: appends BatchReport rows to pipeline_metrics (ADR-008).

Append-only by design. The consumer calls this *after* the offset commit, so a
crash in that window loses one metrics row (a gap, never a duplicate). Metrics
are informational — bronze correctness is unaffected either way.
"""

from datetime import UTC, datetime
from pathlib import Path

from pyiceberg.table import Table

from pipeline.common.config import Settings
from pipeline.common.iceberg import (
    append_arrow,
    ensure_ops_tables,
    load_lake_catalog,
    metrics_to_arrow,
)
from pipeline.common.logging import get_logger
from pipeline.common.models import BatchReport

logger = get_logger("consumer.ops")


class OpsWriter:
    """Owns the pipeline_metrics ops table handle; `append_metrics` adds one row."""

    def __init__(self, settings: Settings, warehouse_dir: Path | None = None) -> None:
        catalog = load_lake_catalog(settings, warehouse_dir)
        tables = ensure_ops_tables(catalog, settings.glue_database)
        self._table: Table = tables.metrics
        logger.info(
            "ops_metrics_ready",
            table=f"{settings.glue_database}.pipeline_metrics",
        )

    def append_metrics(self, report: BatchReport) -> None:
        """Append one BatchReport row. Failures are logged by the caller."""
        append_arrow(self._table, metrics_to_arrow(report, datetime.now(UTC)))
