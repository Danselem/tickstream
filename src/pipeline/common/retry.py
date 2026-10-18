"""Transient-failure retry with exponential backoff + jitter (tenacity).

Covers the failure modes of talking to AWS from short-lived processes: S3
socket timeouts surface as ``OSError`` (pyarrow included), throttling/5xx as
botocore ``ClientError``, and concurrent-writer conflicts as PyIceberg
``CommitFailed``. Anything else (auth, validation, programming errors) fails
fast — retrying those only burns money and hides bugs.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pyiceberg.exceptions import CommitFailedException
from tenacity import (
    Retrying,
    before_sleep_log,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from pipeline.common.logging import get_logger

logger = get_logger("common.retry")

# Botocore error codes worth retrying (timeouts, throttling, 5xx-class).
_RETRYABLE_CODES = frozenset(
    {
        "RequestTimeout",
        "RequestTimeoutException",
        "Throttling",
        "ThrottlingException",
        "ThrottledException",
        "TooManyRequestsException",
        "PriorRequestNotComplete",
        "InternalError",
        "InternalFailure",
        "ServiceUnavailable",
        "SlowDown",
        "OperationTimeout",
    }
)


def is_transient(exc: BaseException) -> bool:
    """True when retrying the same call later could plausibly succeed."""
    if isinstance(exc, OSError):
        # Socket/connection timeouts, incl. pyarrow S3 curlCode 28 failures.
        return True
    if isinstance(exc, CommitFailedException):
        # Lost a concurrent-commit race (e.g. OPTIMIZE vs upsert); the next
        # attempt re-reads the table and usually wins.
        return True
    if type(exc).__name__ == "ClientError" and hasattr(exc, "response"):
        # Duck-typed botocore ClientError (avoids a hard boto dependency here).
        # hasattr() narrows the type for mypy; the payload shape is unchecked.
        response: Any = exc.response
        error = response.get("Error", {}) if isinstance(response, dict) else {}
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        return error.get("Code") in _RETRYABLE_CODES or status >= 500
    return False


def with_transient_retry[T](
    fn: Callable[[], T],
    *,
    action: str,
    attempts: int = 8,
    wait: Any = None,
) -> T:
    """Run ``fn``, retrying transient failures (∼4 min total by default).

    Reraises the last error when attempts run out (or immediately for
    non-transient errors) — the caller decides what a hard failure means
    (consumer: crash without committing offsets; replay absorbs it).
    """
    retrying = Retrying(
        retry=retry_if_exception(is_transient),
        stop=stop_after_attempt(attempts),
        wait=wait if wait is not None else wait_exponential_jitter(initial=1.0, max=60.0),
        before_sleep=before_sleep_log(logger, 20),  # INFO: attempt, delay, error
        reraise=True,
    )
    for attempt in retrying:
        with attempt:
            return fn()
    raise AssertionError("unreachable: tenacity always returns or reraises")
