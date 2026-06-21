"""Issue #19 tests: stage timeouts and transient retry policies."""

from __future__ import annotations

import asyncio

import pytest

from server.resilience.policies import (
    StageTimeoutError,
    is_transient_error,
    run_with_timeout_and_retry,
    stream_with_deadline,
)


def test_is_transient_error_classifies_provider_blips() -> None:
    assert is_transient_error(ConnectionError("reset")) is True

    class _HttpErr(Exception):
        status_code = 503

    assert is_transient_error(_HttpErr()) is True

    class _BadRequest(Exception):
        status_code = 400

    assert is_transient_error(_BadRequest()) is False
    assert is_transient_error(StageTimeoutError("asr", 15.0)) is False


def test_run_with_timeout_and_retry_times_out() -> None:
    async def slow() -> str:
        await asyncio.sleep(0.05)
        return "late"

    with pytest.raises(StageTimeoutError):
        asyncio.run(run_with_timeout_and_retry(slow, stage="asr", timeout_s=0.01, max_retries=0))


def test_run_with_timeout_and_retry_recovers_once() -> None:
    calls = 0

    async def flaky() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("blip")
        return "ok"

    result = asyncio.run(
        run_with_timeout_and_retry(
            flaky,
            stage="llm",
            timeout_s=1.0,
            max_retries=1,
            backoff_ms=(1, 1),
        )
    )
    assert result == "ok"
    assert calls == 2


def test_stream_with_deadline_enforces_ttft() -> None:
    async def slow_stream():
        await asyncio.sleep(0.05)
        yield "token"
        if False:  # pragma: no cover
            yield "never"

    async def run() -> None:
        with pytest.raises(StageTimeoutError) as exc_info:
            async for _ in stream_with_deadline(
                slow_stream(),
                stage="llm",
                timeout_s=1.0,
                ttft_timeout_s=0.01,
            ):
                pass
        assert exc_info.value.ttft is True

    asyncio.run(run())
