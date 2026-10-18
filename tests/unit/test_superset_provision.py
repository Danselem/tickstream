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
    chart_params,
    chart_specs,
    dashboard_layout,
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


def test_chart_specs_cover_brief_panels() -> None:
    specs = chart_specs(["XRPUSDT"])
    assert len(specs) == 5
    assert {spec.dataset for spec in specs} == {
        provision.GOLD_TABLE,
        provision.QUALITY_TABLE,
        provision.METRICS_TABLE,
    }
    scoped = [
        "XRPUSDT" in spec.name or "health" in spec.name.lower() or "lag" in spec.name.lower()
        for spec in specs
    ]
    assert all(scoped)


def test_chart_specs_single_symbol_filters() -> None:
    specs = chart_specs(["XRPUSDT"])
    price = next(spec for spec in specs if spec.viz_type == "echarts_timeseries_line")
    params = chart_params(price, 9)
    assert params["adhoc_filters"][0]["comparator"] == "XRPUSDT"
    assert "groupby" not in params


def test_chart_specs_multi_symbol_groups_by_symbol() -> None:
    specs = chart_specs(["BTCUSDT", "ETHUSDT"])
    assert len(specs) == 5
    coin_specs = [spec for spec in specs if spec.dataset == provision.GOLD_TABLE]
    assert len(coin_specs) == 3
    for spec in coin_specs:
        params = chart_params(spec, 9)
        assert params["groupby"] == ["symbol"]
        assert "adhoc_filters" not in params
    lag = next(spec for spec in specs if spec.dataset == provision.METRICS_TABLE)
    lag_params = chart_params(lag, 9)
    assert "groupby" not in lag_params
    assert "adhoc_filters" not in lag_params


def test_chart_params_renders_table_and_timeseries() -> None:
    specs = {spec.viz_type: spec for spec in chart_specs(["XRPUSDT"])}
    table = chart_params(specs["table"], 7)
    assert table["datasource"] == "7__table"
    assert table["query_mode"] == "raw"
    assert "check_at" in table["columns"]

    line_spec = next(
        spec for spec in chart_specs(["XRPUSDT"]) if spec.name.endswith("(1m)")
    )
    line = chart_params(line_spec, 9)
    assert line["datasource"] == "9__table"
    assert line["x_axis"] == "candle_start"
    assert len(line["metrics"]) >= 1
    assert line["adhoc_filters"][0]["comparator"] == "XRPUSDT"


def test_dashboard_layout_two_per_row_with_chart_ids() -> None:
    charts = [(11, "a"), (12, "b"), (13, "c"), (14, "d"), (15, "e")]
    layout = dashboard_layout(charts)
    rows = [layout[f"ROW-{i}"] for i in range(3)]
    assert [len(row["children"]) for row in rows] == [2, 2, 1]
    assert layout["GRID_ID"]["children"] == ["ROW-0", "ROW-1", "ROW-2"]
    assert layout["ROOT_ID"]["children"] == ["GRID_ID"]
    chart_ids = {
        node["meta"]["chartId"]
        for key, node in layout.items()
        if key.startswith("CHART-")
    }
    assert chart_ids == {11, 12, 13, 14, 15}


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

    def open(
        self, request: urllib.request.Request, timeout: float = 60
    ) -> FakeResponse:
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
        },
    )
    chart_id = api.ensure_chart("c", "table", 7, {"a": 1})
    assert chart_id == 11
    assert api.ensure_dashboard([(11, "c")]) == 21
    posts = [call for call in transport.calls if call[0] == "POST"]
    chart_post = next(call for call in posts if call[1].endswith("/api/v1/chart/"))
    assert json.loads(chart_post[2]["params"]) == {"a": 1}
    dash_post = next(call for call in posts if call[1].endswith("/api/v1/dashboard/"))
    layout = json.loads(dash_post[2]["position_json"])
    assert layout["CHART-11"]["meta"]["chartId"] == 11
    assert dash_post[2]["published"] is True


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
