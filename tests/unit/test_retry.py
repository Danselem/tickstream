"""Unit tests for transient retry (no network; controlled failures)."""

from __future__ import annotations

import pytest
from pyiceberg.exceptions import CommitFailedException
from tenacity import wait_none

from pipeline.common.retry import is_transient, with_transient_retry


class ClientError(Exception):
    """Duck-typed botocore ClientError (name + .response is what matters)."""

    def __init__(self, code: str, status: int) -> None:
        super().__init__(code)
        self.response = {
            "Error": {"Code": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        }


def test_transient_network_and_commit_conflicts() -> None:
    assert is_transient(OSError("curlCode: 28, Timeout was reached"))
    assert is_transient(TimeoutError("timed out"))
    assert is_transient(ConnectionError("reset by peer"))
    assert is_transient(CommitFailedException("conflict"))


def test_transient_retryable_client_errors() -> None:
    assert is_transient(ClientError("ThrottlingException", 400))
    assert is_transient(ClientError("RequestTimeout", 408))
    assert is_transient(ClientError("InternalError", 500))
    assert is_transient(ClientError("SlowDown", 503))


def test_non_transient_fails_fast() -> None:
    assert not is_transient(ValueError("bad payload"))
    assert not is_transient(KeyError("missing"))
    assert not is_transient(ClientError("AccessDenied", 400))
    assert not is_transient(ClientError("ValidationError", 400))


def test_retry_succeeds_after_transient_failures() -> None:
    calls: list[int] = []

    def flaky() -> str:
        calls.append(1)
        if len(calls) < 3:
            raise OSError("blip")
        return "ok"

    assert with_transient_retry(flaky, action="test", attempts=5, wait=wait_none()) == "ok"
    assert len(calls) == 3


def test_non_transient_reraises_immediately() -> None:
    calls: list[int] = []

    def broken() -> str:
        calls.append(1)
        raise ValueError("permanent")

    with pytest.raises(ValueError, match="permanent"):
        with_transient_retry(broken, action="test", attempts=5, wait=wait_none())
    assert len(calls) == 1


def test_retry_exhausts_attempts_then_reraises() -> None:
    calls: list[int] = []

    def down() -> str:
        calls.append(1)
        raise OSError("still down")

    with pytest.raises(OSError, match="still down"):
        with_transient_retry(down, action="test", attempts=4, wait=wait_none())
    assert len(calls) == 4
