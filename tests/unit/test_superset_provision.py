"""Unit tests for the Superset provisioning script (no network; faked transport)."""

from __future__ import annotations

import json
import urllib.request
from typing import Any

import pytest

from pipeline.superset import provision
from pipeline.superset.provision import (
    Api,
    adhoc_equals,
    adhoc_metric,
    athena_uri,
    big_number_params,
    candlestick_params,
    chart_params,
    chart_specs,
    dashboard_layout,
    native_filter_configuration,
    tab_key,
    treemap_params,
)


def test_athena_uri_encodes_staging_dir_and_workgroup() -> None:
    uri = athena_uri(
        access_key="AKID",
        secret_key="s3cr/t+key=",
        region="us-east-1",
        schema="tickstream",
        s3_staging_dir="s3://bucket/athena-results/",
        work_group="tickstream-pipeline",
    )
    assert uri.startswith("awsathena+rest://AKID:")
    assert "s3%3A//bucket/athena-results/" in uri
    assert "work_group=tickstream-pipeline" in uri
    assert "s3cr%2Ft%2Bkey%3D" in uri  # secret fully percent-encoded
    assert "catalog_name=AwsDataCatalog" in uri  # exact case (see docstring)


def test_adhoc_builders_shape() -> None:
    assert adhoc_metric("close", "AVG") == {
        "label": "AVG(close)",
        "expressionType": "SIMPLE",
        "column": {"column_name": "close"},
        "aggregate": "AVG",
    }
    filt = adhoc_equals("symbol", "XRPUSDT")
    assert (filt["subject"], filt["operator"], filt["comparator"]) == (
        "symbol",
        "==",
        "XRPUSDT",
    )


def test_chart_specs_cover_both_tabs() -> None:
    specs = chart_specs(["XRPUSDT"])
    assert len(specs) == 20
    assert {spec.tab for spec in specs} == {provision.TAB_MARKET, provision.TAB_HEALTH}
    assert {spec.dataset for spec in specs} == {
        provision.GOLD_TABLE,
        provision.UNIFIED_TABLE,
        provision.DAILY_SUMMARY_TABLE,
        provision.HOURLY_SUMMARY_TABLE,
        provision.QUALITY_TABLE,
        provision.METRICS_TABLE,
        provision.INTEGRITY_TABLE,
        provision.FRESHNESS_TABLE,
        provision.LAYER_COUNTS_TABLE,
        provision.MICRO_TABLE,
    }
    # Market tab is business-facing; pipeline health tab is the ops view.
    market = [spec for spec in specs if spec.tab == provision.TAB_MARKET]
    health = [spec for spec in specs if spec.tab == provision.TAB_HEALTH]
    assert len(market) == 14 and len(health) == 6
    # The candlestick and the two summary tables span a full row.
    wide = [spec.name for spec in market if spec.wide]
    assert wide == [
        "XRPUSDT price — candlestick (1m)",
        "Market summary — last 24h (UTC)",
        "Market summary — hourly (last 24h)",
    ]
    # Summary/KPI charts use a 24h trailing window; everything else 7d.
    summary = {
        spec.name
        for spec in specs
        if spec.time_range == provision.SUMMARY_TIME_RANGE
        and spec.dataset in (provision.DAILY_SUMMARY_TABLE, provision.HOURLY_SUMMARY_TABLE)
    }
    assert summary == {
        "Market summary — last 24h (UTC)",
        "Market summary — hourly (last 24h)",
        "Notional share by coin (last 24h)",
        "Total notional (USD, last 24h)",
        "Trades (last 24h)",
    }


def test_chart_specs_single_symbol_filters() -> None:
    specs = chart_specs(["XRPUSDT"])
    price = next(spec for spec in specs if spec.viz_type == "echarts_timeseries_line")
    params = chart_params(price, 9)
    assert params["adhoc_filters"][0]["comparator"] == "XRPUSDT"
    assert "groupby" not in params


def test_chart_specs_multi_symbol_groups_by_symbol() -> None:
    specs = chart_specs(["BTCUSDT", "ETHUSDT"])
    assert len(specs) == 20
    coin_specs = [
        spec for spec in specs if spec.dataset in (provision.GOLD_TABLE, provision.UNIFIED_TABLE)
    ]
    assert len(coin_specs) == 7
    for spec in coin_specs:
        params = chart_params(spec, 9)
        if spec.viz_type == "candlestick":
            assert params["series"] == "symbol"
            continue
        assert params["groupby"] == ["symbol"]
        assert "adhoc_filters" not in params
    micro_specs = [spec for spec in specs if spec.dataset == provision.MICRO_TABLE]
    assert len(micro_specs) == 2
    for spec in micro_specs:
        params = chart_params(spec, 10)
        assert params["groupby"] == ["symbol"]

    metrics_specs = [spec for spec in specs if spec.dataset == provision.METRICS_TABLE]
    assert len(metrics_specs) == 2  # lag and batch rows are separate charts
    for spec in metrics_specs:
        params = chart_params(spec, 9)
        assert "groupby" not in params
        assert "adhoc_filters" not in params


def test_chart_params_renders_table_and_timeseries() -> None:
    specs = chart_specs(["XRPUSDT"])
    quality = next(spec for spec in specs if spec.dataset == provision.QUALITY_TABLE)
    table = chart_params(quality, 7)
    assert table["datasource"] == "7__table"
    assert table["query_mode"] == "raw"
    assert "check_at" in table["columns"]

    line_spec = next(spec for spec in specs if spec.dataset == provision.GOLD_TABLE)
    line = chart_params(line_spec, 9)
    assert line["datasource"] == "9__table"
    assert line["x_axis"] == "candle_start"
    assert len(line["metrics"]) >= 1
    assert line["adhoc_filters"][0]["comparator"] == "XRPUSDT"


def test_timeseries_params_carry_labels_and_legend() -> None:
    """Readability: axis titles, legend and time format must be explicit."""
    specs = chart_specs(["XRPUSDT"])
    price = next(spec for spec in specs if spec.dataset == provision.UNIFIED_TABLE)
    params = chart_params(price, 9)
    assert params["x_axis_title"] == "Time (UTC)"
    assert params["y_axis_title"] == "Price (USDT)"
    assert params["show_legend"] is True
    assert params["legendType"] == "scroll"
    assert params["x_axis_time_format"] == "%H:%M"
    assert params["x_axis_title_margin"] == 30
    assert params["y_axis_title_margin"] == 60
    assert params["rich_tooltip"] is True
    assert params["color_scheme"] == "supersetColors"


def test_table_params_get_readability_defaults() -> None:
    specs = chart_specs(["XRPUSDT"])
    quality = next(spec for spec in specs if spec.dataset == provision.QUALITY_TABLE)
    params = chart_params(quality, 7)
    assert params["table_timestamp_format"] == "%Y-%m-%d %H:%M"
    assert params["show_cell_bars"] is True


def test_candlestick_params_shape() -> None:
    params = candlestick_params(
        9,
        x_axis="candle_start",
        open_column="open",
        close_column="close",
        high_column="high",
        low_column="low",
        series="symbol",
        time_grain_sqla="PT1M",
        y_axis_format=",.4f",
    )
    assert params["viz_type"] == "candlestick"
    assert params["open"]["aggregate"] == "MAX" and params["open"]["label"] == "Open"
    assert params["low"]["aggregate"] == "MIN"
    assert params["series"] == "symbol"
    assert params["show_x_axis"] is True and params["show_y_axis"] is True
    assert params["x_axis_title_margin"] == 30


def test_treemap_and_big_number_params_shape() -> None:
    tree = treemap_params(2, groupby=["symbol"], metric=adhoc_metric("notional", "SUM"))
    assert tree["viz_type"] == "treemap_v2"
    assert tree["groupby"] == ["symbol"]
    assert tree["sort_by_metric"] is True

    big = big_number_params(2, metric=adhoc_metric("notional", "SUM"), y_axis_format="$,.0f")
    assert big["viz_type"] == "big_number_total"
    assert big["metric"]["label"] == "SUM(notional)"


def test_native_filter_configuration_targets_and_scope() -> None:
    filters = native_filter_configuration(
        symbol_dataset_uuid="uuid-1",
        gold_chart_ids=[1, 2, 6],
        all_chart_ids=[1, 2, 3],
        market_tab="TAB-Market",
        health_tab="TAB-Pipeline-health",
    )
    by_id = {flt["id"]: flt for flt in filters}
    symbol = by_id["NATIVE_FILTER-symbol"]
    assert symbol["filterType"] == "filter_select"
    assert symbol["targets"] == [{"column": {"name": "symbol"}, "datasetUuid": "uuid-1"}]
    assert symbol["chartsInScope"] == [1, 2, 6]
    assert symbol["tabsInScope"] == ["TAB-Market"]
    time = by_id["NATIVE_FILTER-time"]
    assert time["filterType"] == "filter_time"
    assert time["targets"] == [{}]
    assert time["tabsInScope"] == ["TAB-Market", "TAB-Pipeline-health"]


def test_tab_key_slug_is_stable() -> None:
    assert tab_key("Market") == "TAB-Market"
    assert tab_key("Pipeline health") == "TAB-Pipeline-health"


def test_label_columns_resends_full_list_preserving_dttm() -> None:
    api, transport = _api_with(
        {
            ("GET", "/api/v1/dataset/4"): {
                "result": {
                    "columns": [
                        {
                            "id": 10,
                            "column_name": "check_at",
                            "is_dttm": True,
                            "type": "TIMESTAMP",
                        },
                        {"id": 11, "column_name": "success", "is_dttm": False},
                    ]
                }
            },
            ("PUT", "/api/v1/dataset/4"): {"id": 4},
        },
    )
    api.label_columns(4, {"success": "Passed"})
    put = next(call for call in transport.calls if call[0] == "PUT")
    columns = {col["column_name"]: col for col in put[2]["columns"]}
    assert columns["check_at"]["is_dttm"] is True
    assert columns["check_at"]["verbose_name"] is None  # not in the label map
    assert columns["success"]["verbose_name"] == "Passed"
    assert columns["check_at"]["id"] == 10  # existing id kept (avoids re-insert)
    assert columns["success"]["id"] == 11
    assert set(columns) == {"check_at", "success"}


def test_dashboard_layout_builds_tabs_and_wide_rows() -> None:
    layout = dashboard_layout(
        [
            ("Market", [(1, "a", False), (2, "b", False), (3, "big", True)]),
            ("Pipeline health", [(4, "c", False), (5, "d", False)]),
        ]
    )
    assert layout["ROOT_ID"]["children"] == ["TABS-main"]
    assert layout["TABS-main"]["children"] == [
        "TAB-Market",
        "TAB-Pipeline-health",
    ]
    assert layout["TAB-Market"]["meta"]["text"] == "Market"
    assert layout["TAB-Pipeline-health"]["children"] == ["ROW-1-0"]
    assert layout["TAB-Market"]["children"] == ["ROW-0-0", "ROW-0-1"]
    assert layout["ROW-0-0"]["children"] == ["CHART-1", "CHART-2"]
    assert layout["ROW-0-1"]["children"] == ["CHART-3"]
    assert layout["CHART-1"]["meta"]["width"] == 6
    assert layout["CHART-3"]["meta"]["width"] == 12
    assert layout["GRID_ID"]["children"] == []


class FakeResponse:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode()

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class FakeTransport:
    """Canned (method, path-prefix) -> response payload; records all calls."""

    def __init__(self, routes: dict[tuple[str, str], Any]) -> None:
        self.routes = routes
        self.calls: list[tuple[str, str, Any]] = []

    def open(self, request: urllib.request.Request, timeout: float = 60) -> FakeResponse:
        assert isinstance(request, urllib.request.Request)
        assert request.data is None or isinstance(request.data, bytes)
        body = json.loads(request.data.decode()) if request.data else None
        self.calls.append((request.get_method(), request.full_url, body))
        for (method, prefix), payload in self.routes.items():
            if request.get_method() == method and prefix in request.full_url:
                return FakeResponse(payload() if callable(payload) else payload)
        raise AssertionError(f"unexpected call {request.get_method()} {request.full_url}")


def _api_with(routes: dict[tuple[str, str], Any]) -> tuple[Api, FakeTransport]:
    transport = FakeTransport(routes)
    return Api("http://localhost:8088", opener=transport), transport


def test_ensure_database_updates_existing() -> None:
    api, transport = _api_with(
        {
            ("GET", "/api/v1/database/"): {"result": [{"id": 3}]},
            ("PUT", "/api/v1/database/3"): {"id": 3},
        },
    )
    assert api.ensure_database("tickstream_athena", "uri") == 3
    methods = [call[0] for call in transport.calls]
    assert methods == ["GET", "PUT"]


def test_ensure_database_creates_missing() -> None:
    api, _transport = _api_with(
        {
            ("GET", "/api/v1/database/"): {"result": []},
            ("POST", "/api/v1/database/"): {"id": 5},
        },
    )
    assert api.ensure_database("tickstream_athena", "uri") == 5


def test_ensure_chart_and_dashboard_create_path() -> None:
    api, transport = _api_with(
        {
            ("GET", "/api/v1/chart/"): {"result": []},
            ("POST", "/api/v1/chart/"): {"id": 11},
            ("GET", "/api/v1/dashboard/"): {"result": []},
            ("POST", "/api/v1/dashboard/"): {"id": 21},
            ("PUT", "/api/v1/dashboard/21"): {"id": 21},
        },
    )
    chart_id = api.ensure_chart("c", "table", 7, {"a": 1})
    assert chart_id == 11
    assert api.ensure_dashboard([("Market", [(11, "c", False)])]) == 21
    posts = [call for call in transport.calls if call[0] == "POST"]
    chart_post = next(call for call in posts if call[1].endswith("/api/v1/chart/"))
    assert json.loads(chart_post[2]["params"]) == {"a": 1}
    dash_post = next(call for call in posts if call[1].endswith("/api/v1/dashboard/"))
    layout = json.loads(dash_post[2]["position_json"])
    assert layout["CHART-11"]["meta"]["chartId"] == 11
    assert dash_post[2]["published"] is True
    # Create alone does not link charts; a follow-up PUT carries the layout so
    # set_dash_metadata populates dashboard_slices.
    put = next(call for call in transport.calls if call[0] == "PUT")
    assert json.loads(put[2]["json_metadata"])["positions"]["CHART-11"]["meta"]["chartId"] == 11


def test_ensure_chart_finds_name_with_plus_without_duplicating() -> None:
    """The server-side slice_name filter drops "+"; match in Python instead."""
    api, transport = _api_with(
        {
            ("GET", "/api/v1/chart/"): {
                "result": [{"id": 7, "slice_name": "Consumer lag + batch rows"}]
            },
            ("PUT", "/api/v1/chart/7"): {"id": 7},
        },
    )
    assert api.ensure_chart("Consumer lag + batch rows", "line", 1, {}) == 7
    assert [call[0] for call in transport.calls] == ["GET", "PUT"]


def test_ensure_dashboard_update_omits_charts_field() -> None:
    """Superset 6.0's dashboard PUT schema has no `charts`; linkage comes from
    json_metadata.positions."""
    api, transport = _api_with(
        {
            ("GET", "/api/v1/dashboard/"): {"result": [{"id": 21}]},
            ("PUT", "/api/v1/dashboard/21"): {"id": 21},
        },
    )
    assert api.ensure_dashboard([("Market", [(11, "c", False), (12, "d", False)])]) == 21
    put = next(call for call in transport.calls if call[0] == "PUT")
    assert "charts" not in put[2]
    assert "position_json" in put[2] and put[2]["published"] is True
    positions = json.loads(put[2]["json_metadata"])["positions"]
    assert positions["CHART-11"]["meta"]["chartId"] == 11
    assert positions["CHART-12"]["meta"]["chartId"] == 12


def test_login_sets_bearer_token() -> None:
    api, _transport = _api_with(
        {
            ("POST", "/api/v1/security/login"): {"access_token": "tok"},
            ("GET", "/api/v1/security/csrf_token/"): {"result": "csrf123"},
        },
    )
    api.login("admin", "pw")
    assert api.token == "tok"
    assert api.csrf_token == "csrf123"


def test_main_requires_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TICKSTREAM_SYMBOLS", raising=False)
    with pytest.raises(provision.ProvisionError):
        provision.main()


def test_provision_module_imports_without_requests() -> None:
    with open(provision.__file__, encoding="utf-8") as fh:
        source = fh.read()
    assert "import requests" not in source


def test_native_filter_defaults_to_btc() -> None:
    filters = native_filter_configuration(
        symbol_dataset_uuid="uuid-1",
        gold_chart_ids=[1],
        all_chart_ids=[1],
        market_tab="TAB-Market",
        health_tab="TAB-Pipeline-health",
        default_symbol="BTCUSDT",
    )
    symbol = next(f for f in filters if f["id"] == "NATIVE_FILTER-symbol")
    assert symbol["defaultDataMask"]["filterState"]["value"] == ["BTCUSDT"]
    assert symbol["defaultDataMask"]["extraFormData"]["filters"] == [
        {"col": "symbol", "op": "IN", "val": ["BTCUSDT"]}
    ]


def test_market_charts_use_detail_and_trend_windows() -> None:
    specs = chart_specs(["BTCUSDT"])
    trend = next(spec for spec in specs if "30d trend" in spec.name)
    assert trend.dataset == provision.UNIFIED_TABLE
    assert trend.time_range == provision.TREND_TIME_RANGE
    detail = [spec for spec in specs if spec.dataset == provision.UNIFIED_TABLE]
    assert len(detail) == 6
    for spec in detail:
        assert spec.time_range in (provision.DETAIL_TIME_RANGE, provision.TREND_TIME_RANGE)
