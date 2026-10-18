"""Idempotent Superset provisioning: Athena database, datasets, charts, dashboard.

Runs inside the superset container at boot (``bootstrap.sh``) and on demand via
``make superset``. Only stdlib imports — the superset image has no project venv.

Everything is create-if-missing / update-if-present, keyed by stable names, so
restarts and rebuilds converge without duplicating assets.
"""

from __future__ import annotations

import http.cookiejar
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

DATABASE_NAME = "tickstream_athena"
GOLD_TABLE = "gold_candles_1m"
DAILY_SUMMARY_TABLE = "gold_market_summary_daily"
HOURLY_SUMMARY_TABLE = "gold_market_summary_hourly"
QUALITY_TABLE = "quality_results"
METRICS_TABLE = "pipeline_metrics"
INTEGRITY_TABLE = "integrity_results"
DASHBOARD_SLUG = "tickstream"
DASHBOARD_TITLE = "Tickstream — crypto pipeline"

# Dashboard tabs (order preserved in the layout).
TAB_MARKET = "Market"
TAB_HEALTH = "Pipeline health"
TAB_ORDER = (TAB_MARKET, TAB_HEALTH)

# Chart specs feed the dataset ids in this order.
DATASET_TABLES = (
    GOLD_TABLE,
    DAILY_SUMMARY_TABLE,
    HOURLY_SUMMARY_TABLE,
    QUALITY_TABLE,
    METRICS_TABLE,
    INTEGRITY_TABLE,
)

TIME_RANGE = "Last day"


class ProvisionError(RuntimeError):
    """Superset API call failed; carries status + response body for debugging."""


# ---------------------------------------------------------------------------
# Pure builders (unit-tested; no I/O)
# ---------------------------------------------------------------------------


def athena_uri(
    *,
    access_key: str,
    secret_key: str,
    region: str,
    schema: str,
    s3_staging_dir: str,
    work_group: str,
) -> str:
    """SQLAlchemy URI for the Superset Athena database (PyAthena REST dialect).

    Only ``:`` is percent-encoded in the staging dir (``s3%3A//...``); slashes
    stay literal, per the Superset Athena docs. ``work_group`` rides along as a
    query parameter, which PyAthena forwards to ``connect()``.

    ``catalog_name`` is pinned to the exact case (``AwsDataCatalog``): PyAthena
    defaults it to lowercase ``awsdatacatalog``, and Athena evaluates
    GetTableMetadata/ListDatabases against ``datacatalog/<name>``
    case-sensitively, so the lowercase default is denied by our policy.
    """
    user = urllib.parse.quote(access_key, safe="")
    password = urllib.parse.quote(secret_key, safe="")
    staging = s3_staging_dir.replace("://", "%3A//")
    return (
        f"awsathena+rest://{user}:{password}"
        f"@athena.{region}.amazonaws.com/{schema}"
        f"?s3_staging_dir={staging}&work_group={work_group}"
        f"&catalog_name=AwsDataCatalog"
    )


def adhoc_metric(column: str, aggregate: str) -> dict[str, Any]:
    """Minimal SIMPLE metric; Superset resolves the column against the dataset."""
    return {
        "label": f"{aggregate}({column})",
        "expressionType": "SIMPLE",
        "column": {"column_name": column},
        "aggregate": aggregate,
    }


def adhoc_equals(column: str, value: str) -> dict[str, Any]:
    return {
        "expressionType": "SIMPLE",
        "subject": column,
        "operator": "==",
        "comparator": value,
        "clause": "WHERE",
    }


def _base_params(
    dataset_id: int, viz_type: str, *, time_range: str = TIME_RANGE
) -> dict[str, Any]:
    return {
        "datasource": f"{dataset_id}__table",
        "viz_type": viz_type,
        "time_range": time_range,
        "row_limit": 10000,
    }


def timeseries_params(
    dataset_id: int,
    viz_type: str,
    *,
    x_axis: str,
    metrics: list[dict[str, Any]],
    filters: list[dict[str, Any]] | None = None,
    groupby: list[str] | None = None,
    time_grain_sqla: str | None = None,
    y_axis_format: str | None = None,
) -> dict[str, Any]:
    params = _base_params(dataset_id, viz_type)
    params["x_axis"] = x_axis
    params["metrics"] = metrics
    if filters:
        params["adhoc_filters"] = filters
    if groupby:
        params["groupby"] = groupby
    if time_grain_sqla:
        params["time_grain_sqla"] = time_grain_sqla
    if y_axis_format:
        params["y_axis_format"] = y_axis_format
    return params


def table_params(
    dataset_id: int, *, columns: list[str], order_by: str, row_limit: int = 20
) -> dict[str, Any]:
    params = _base_params(dataset_id, "table")
    params["query_mode"] = "raw"
    params["columns"] = columns
    params["orderby"] = [[order_by, False]]
    params["row_limit"] = row_limit
    params["server_pagination"] = True
    return params


@dataclass(frozen=True)
class ChartSpec:
    """One dashboard chart: stable name + tab + viz + params builder arguments."""

    name: str
    tab: str
    viz_type: str
    dataset: str
    params: dict[str, Any]
    wide: bool = False  # tables span a full row


def chart_specs(symbols: list[str]) -> list[ChartSpec]:
    """Business "Market" tab + engineering "Pipeline health" tab.

    One symbol → symbol-filtered market charts; several → one line per symbol via
    groupby (no chart sprawl as coins are added)."""
    if len(symbols) == 1:
        coin: list[dict[str, Any]] = [adhoc_equals("symbol", symbols[0])]
        groupby: list[str] | None = None
        prefix = f"{symbols[0]} "
    else:
        coin = []
        groupby = ["symbol"]
        prefix = ""
    view = {
        "filters": coin,
        "groupby": groupby,
    }
    return [
        ChartSpec(
            name=f"{prefix}price — close vs VWAP (1m)",
            tab=TAB_MARKET,
            viz_type="echarts_timeseries_line",
            dataset=GOLD_TABLE,
            params={
                "x_axis": "candle_start",
                "time_grain_sqla": "PT1M",
                "metrics": [adhoc_metric("close", "AVG"), adhoc_metric("vwap", "AVG")],
                "y_axis_format": ",.4f",
                **view,
            },
        ),
        ChartSpec(
            name=f"{prefix}base volume (1m)",
            tab=TAB_MARKET,
            viz_type="echarts_timeseries_bar",
            dataset=GOLD_TABLE,
            params={
                "x_axis": "candle_start",
                "time_grain_sqla": "PT1M",
                "metrics": [adhoc_metric("volume", "SUM")],
                "y_axis_format": ",.0f",
                **view,
            },
        ),
        ChartSpec(
            name=f"{prefix}USD notional traded (1m)",
            tab=TAB_MARKET,
            viz_type="echarts_timeseries_bar",
            dataset=GOLD_TABLE,
            params={
                "x_axis": "candle_start",
                "time_grain_sqla": "PT1M",
                "metrics": [adhoc_metric("notional", "SUM")],
                "y_axis_format": "$,.0f",
                **view,
            },
        ),
        ChartSpec(
            name=f"{prefix}trades per minute (1m)",
            tab=TAB_MARKET,
            viz_type="echarts_timeseries_line",
            dataset=GOLD_TABLE,
            params={
                "x_axis": "candle_start",
                "time_grain_sqla": "PT1M",
                "metrics": [adhoc_metric("trade_count", "SUM")],
                "y_axis_format": ",.0f",
                **view,
            },
        ),
        ChartSpec(
            name=f"{prefix}volatility (1m log-return stddev)",
            tab=TAB_MARKET,
            viz_type="echarts_timeseries_line",
            dataset=GOLD_TABLE,
            params={
                "x_axis": "candle_start",
                "time_grain_sqla": "PT1M",
                "metrics": [adhoc_metric("volatility", "AVG")],
                "y_axis_format": ".2e",
                **view,
            },
        ),
        ChartSpec(
            name="Market summary — today (UTC)",
            tab=TAB_MARKET,
            viz_type="table",
            dataset=DAILY_SUMMARY_TABLE,
            wide=True,
            params={
                "columns": [
                    "symbol",
                    "window_start",
                    "open",
                    "close",
                    "change_pct",
                    "high",
                    "low",
                    "notional",
                    "trade_count",
                    "trades_per_min",
                ],
                "order_by": "window_start",
                "row_limit": 50,
            },
        ),
        ChartSpec(
            name="Market summary — hourly",
            tab=TAB_MARKET,
            viz_type="table",
            dataset=HOURLY_SUMMARY_TABLE,
            wide=True,
            params={
                "columns": [
                    "symbol",
                    "window_start",
                    "open",
                    "close",
                    "change_pct",
                    "high",
                    "low",
                    "notional",
                    "trade_count",
                    "trades_per_min",
                ],
                "order_by": "window_start",
                "row_limit": 50,
            },
        ),
        ChartSpec(
            name="Consumer lag + batch rows",
            tab=TAB_HEALTH,
            viz_type="echarts_timeseries_line",
            dataset=METRICS_TABLE,
            params={
                "x_axis": "collected_at",
                "metrics": [
                    adhoc_metric("lag_messages", "AVG"),
                    adhoc_metric("rows", "AVG"),
                ],
            },
        ),
        ChartSpec(
            name="Pipeline health — recent quality runs",
            tab=TAB_HEALTH,
            viz_type="table",
            dataset=QUALITY_TABLE,
            params={
                "columns": [
                    "check_at",
                    "success",
                    "window_start",
                    "window_end",
                    "rows_scanned",
                    "run_ms",
                ],
                "order_by": "check_at",
                "row_limit": 20,
            },
        ),
        ChartSpec(
            name="Bronze integrity — duplicate keys over time",
            tab=TAB_HEALTH,
            viz_type="echarts_timeseries_line",
            dataset=INTEGRITY_TABLE,
            params={
                "x_axis": "checked_at",
                "metrics": [adhoc_metric("duplicates", "MAX")],
                "y_axis_format": ",.0f",
            },
        ),
    ]


def chart_params(spec: ChartSpec, dataset_id: int) -> dict[str, Any]:
    """Render a ChartSpec into Superset chart params (table viz differs)."""
    if spec.viz_type == "table":
        assert isinstance(spec.params["columns"], list)
        assert isinstance(spec.params["order_by"], str)
        limit = spec.params.get("row_limit", 20)
        assert isinstance(limit, int)
        return table_params(
            dataset_id,
            columns=spec.params["columns"],
            order_by=spec.params["order_by"],
            row_limit=limit,
        )
    metrics = spec.params.get("metrics", [])
    filters = spec.params.get("filters")
    assert isinstance(metrics, list)
    assert filters is None or isinstance(filters, list)
    dimensions = spec.params.get("groupby")
    assert dimensions is None or isinstance(dimensions, list)
    grain = spec.params.get("time_grain_sqla")
    assert grain is None or isinstance(grain, str)
    y_format = spec.params.get("y_axis_format")
    assert y_format is None or isinstance(y_format, str)
    x_axis = spec.params["x_axis"]
    assert isinstance(x_axis, str)
    return timeseries_params(
        dataset_id,
        spec.viz_type,
        x_axis=x_axis,
        metrics=metrics,
        filters=filters or None,
        groupby=dimensions,
        time_grain_sqla=grain,
        y_axis_format=y_format,
    )


def dashboard_layout(
    tabs: list[tuple[str, list[tuple[int, str, bool]]]],
) -> dict[str, Any]:
    """Native-tab layout: ``ROOT -> TABS -> TAB(text) -> ROW -> CHART``.

    Each tab is ``(title, [(chart_id, name, wide), ...])``. Wide charts (tables)
    span their own full-width row; others pack two per row. Nodes key off the
    numeric chart id; the frontend links layout to slices via ``meta.chartId``."""
    nodes: dict[str, Any] = {}
    tab_keys: list[str] = []
    for t_index, (title, charts) in enumerate(tabs):
        row_ids: list[str] = []
        row_index = 0
        cursor = 0
        while cursor < len(charts):
            if charts[cursor][2]:  # wide table: one per row
                group = charts[cursor : cursor + 1]
                cursor += 1
            else:
                group = charts[cursor : cursor + 2]
                cursor += 2
            row_id = f"ROW-{t_index}-{row_index}"
            children: list[str] = []
            for chart_id, name, wide in group:
                key = f"CHART-{chart_id}"
                children.append(key)
                nodes[key] = {
                    "children": [],
                    "id": key,
                    "meta": {
                        "chartId": chart_id,
                        "height": 50,
                        "sliceName": {"value": name},
                        "width": 12 if wide else 6,
                    },
                    "type": "CHART",
                }
            nodes[row_id] = {
                "children": children,
                "id": row_id,
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
                "type": "ROW",
            }
            row_ids.append(row_id)
            row_index += 1
        tab_key = f"TAB-{t_index}"
        tab_keys.append(tab_key)
        nodes[tab_key] = {
            "children": row_ids,
            "id": tab_key,
            "meta": {"text": title},
            "type": "TAB",
        }
    nodes["TABS-main"] = {
        "children": tab_keys,
        "id": "TABS-main",
        "meta": {},
        "type": "TABS",
    }
    nodes["GRID_ID"] = {"children": [], "id": "GRID_ID", "type": "GRID"}
    nodes["HEADER_ID"] = {
        "id": "HEADER_ID",
        "meta": {"text": DASHBOARD_TITLE},
        "type": "HEADER",
    }
    nodes["ROOT_ID"] = {"children": ["TABS-main"], "id": "ROOT_ID", "type": "ROOT"}
    nodes["DASHBOARD_VERSION_KEY"] = "v2"
    return nodes


# ---------------------------------------------------------------------------
# Thin Superset REST client (urllib; easy to fake in unit tests)
# ---------------------------------------------------------------------------


class Api:
    """Authenticated Superset v1 API with ensure-* (find-or-create) helpers."""

    def __init__(self, base_url: str, opener: Any = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.token: str | None = None
        self.csrf_token: str | None = None
        # Cookie jar: the CSRF token is bound to the login session, so the
        # session cookie must accompany every mutating call. `opener` is a
        # test seam (unit tests inject a fake).
        if opener is None:
            jar = http.cookiejar.CookieJar()
            opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
        self._opener = opener

    # -- low level --------------------------------------------------------
    def _request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> Any:
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            self.base_url + path, data=data, method=method.upper()
        )
        request.add_header("Content-Type", "application/json")
        if self.token:
            request.add_header("Authorization", f"Bearer {self.token}")
        if self.csrf_token:
            request.add_header("X-CSRFToken", self.csrf_token)
        try:
            with self._opener.open(request, timeout=60) as response:
                payload = response.read().decode()
                return json.loads(payload) if payload else None
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode()[:2000]
            raise ProvisionError(f"{method} {path} -> {exc.code}: {detail}") from exc

    def login(self, username: str, password: str) -> None:
        result = self._request(
            "POST",
            "/api/v1/security/login",
            {"username": username, "password": password, "provider": "db"},
        )
        self.token = result["access_token"]
        # Mutating API calls additionally require the CSRF header, even with JWT.
        csrf = self._request("GET", "/api/v1/security/csrf_token/")
        self.csrf_token = csrf["result"]

    def _find(self, resource: str, column: str, value: str) -> dict[str, Any] | None:
        query = json.dumps(
            {"filters": [{"col": column, "opr": "eq", "value": value}]}
        )
        result = self._request(
            "GET", f"/api/v1/{resource}/?q={urllib.parse.quote(query)}"
        )
        items = result.get("result", [])
        return items[0] if items else None

    # -- ensure helpers ----------------------------------------------------
    def ensure_database(self, name: str, uri: str) -> int:
        existing = self._find("database", "database_name", name)
        if existing:
            self._request(
                "PUT",
                f"/api/v1/database/{existing['id']}",
                {"sqlalchemy_uri": uri, "expose_in_sqllab": True},
            )
            return int(existing["id"])
        created = self._request(
            "POST",
            "/api/v1/database/",
            {
                "database_name": name,
                "sqlalchemy_uri": uri,
                "expose_in_sqllab": True,
            },
        )
        return int(created["id"])

    def ensure_dataset(self, database_id: int, schema: str, table: str) -> int:
        # Table names are unique in our lake; match on (schema, table) over the
        # full dataset list (the list API forbids filtering by the `database`
        # relation column).
        for item in self._all_datasets():
            if item.get("schema") == schema and item.get("table_name") == table:
                return int(item["id"])
        created = self._request(
            "POST",
            "/api/v1/dataset/",
            {"database": database_id, "schema": schema, "table_name": table},
        )
        return int(created["id"])

    def _all_datasets(self) -> list[dict[str, Any]]:
        result = self._request("GET", "/api/v1/dataset/")
        items = result.get("result", [])
        return [item for item in items if isinstance(item, dict)]

    def _all_charts(self) -> list[dict[str, Any]]:
        result = self._request("GET", "/api/v1/chart/")
        items = result.get("result", [])
        return [item for item in items if isinstance(item, dict)]

    def _find_chart(self, name: str) -> dict[str, Any] | None:
        # Match in Python, not via the list API's `slice_name eq` filter: that
        # server-side filter returns no rows for names containing "+" (e.g.
        # "Consumer lag + batch rows"), so ensure_chart re-created the chart on
        # every provisioning run (chart sprawl).
        for item in self._all_charts():
            if item.get("slice_name") == name:
                return item
        return None

    def ensure_chart(
        self, name: str, viz_type: str, dataset_id: int, params: dict[str, Any]
    ) -> int:
        existing = self._find_chart(name)
        payload = {
            "slice_name": name,
            "viz_type": viz_type,
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "params": json.dumps(params),
        }
        if existing:
            self._request("PUT", f"/api/v1/chart/{existing['id']}", payload)
            return int(existing["id"])
        created = self._request("POST", "/api/v1/chart/", payload)
        return int(created["id"])

    def ensure_dashboard(
        self, tabs: list[tuple[str, list[tuple[int, str, bool]]]]
    ) -> int:
        layout = dashboard_layout(tabs)
        existing = self._find("dashboard", "slug", DASHBOARD_SLUG)
        payload = {
            "dashboard_title": DASHBOARD_TITLE,
            "slug": DASHBOARD_SLUG,
            "position_json": json.dumps(layout),
            # Superset 6.0 links dashboard<->charts only through
            # DashboardDAO.set_dash_metadata, which UpdateDashboardCommand runs
            # when `json_metadata` is present and reads `positions` from it to
            # populate the `dashboard_slices` association. The write schemas have
            # no `charts` field (read-only in 6.0), so the layout rides here too.
            "json_metadata": json.dumps({"positions": layout}),
            "published": True,
        }
        if existing:
            self._request("PUT", f"/api/v1/dashboard/{existing['id']}", payload)
            return int(existing["id"])
        created = self._request("POST", "/api/v1/dashboard/", payload)
        dashboard_id = int(created["id"])
        # CreateDashboardCommand does NOT sync slices; a follow-up PUT triggers
        # set_dash_metadata so the dashboard actually has its charts linked.
        self._request("PUT", f"/api/v1/dashboard/{dashboard_id}", payload)
        return dashboard_id


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def _env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ProvisionError(f"{name} is required for dashboard provisioning")
    return value


def main() -> None:
    base_url = os.environ.get("SUPERSET_URL", "http://localhost:8088")
    symbols = json.loads(_env("TICKSTREAM_SYMBOLS"))
    if not symbols:
        raise ProvisionError("TICKSTREAM_SYMBOLS is empty")
    uri = athena_uri(
        access_key=_env("AWS_ACCESS_KEY_ID"),
        secret_key=_env("AWS_SECRET_ACCESS_KEY"),
        region=_env("TICKSTREAM_AWS_REGION"),
        schema=_env("TICKSTREAM_GLUE_DATABASE"),
        s3_staging_dir=f"s3://{_env('TICKSTREAM_S3_BUCKET')}/athena-results/",
        work_group=os.environ.get("TICKSTREAM_ATHENA_WORKGROUP", "tickstream-pipeline"),
    )

    api = Api(base_url)
    api.login(_env("SUPERSET_ADMIN_USER"), _env("SUPERSET_ADMIN_PASSWORD"))

    db_id = api.ensure_database(DATABASE_NAME, uri)
    schema = _env("TICKSTREAM_GLUE_DATABASE")
    dataset_ids = {
        table: api.ensure_dataset(db_id, schema, table) for table in DATASET_TABLES
    }

    placed: dict[str, list[tuple[int, str, bool]]] = {tab: [] for tab in TAB_ORDER}
    for spec in chart_specs(symbols):
        dataset_id = dataset_ids[spec.dataset]
        params = chart_params(spec, dataset_id)
        chart_id = api.ensure_chart(spec.name, spec.viz_type, dataset_id, params)
        placed[spec.tab].append((chart_id, spec.name, spec.wide))
        print(f"chart ready: {spec.name} (id={chart_id})", flush=True)

    tabs = [(tab, placed[tab]) for tab in TAB_ORDER if placed[tab]]
    dashboard_id = api.ensure_dashboard(tabs)
    print(
        f"provisioned database={db_id} datasets={dataset_ids} "
        f"dashboard={dashboard_id} at {base_url}/superset/dashboard/{DASHBOARD_SLUG}/",
        flush=True,
    )


if __name__ == "__main__":
    main()
