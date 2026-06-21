"""Stage timeouts and bounded transient retry (issue #19).

Each stage call runs inside a fixed deadline. At most one retry is attempted for
transient provider errors, with short jitter, and only if time remains.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")


class StageTimeoutError(TimeoutError):
    """A stage exceeded its configured deadline."""

    def __init__(self, stage: str, timeout_s: float, *, ttft: bool = False) -> None:
        self.stage = stage
        self.timeout_s = timeout_s
        self.ttft = ttft
        kind = "TTFT " if ttft else ""
        super().__init__(f"{stage} {kind}timed out after {timeout_s}s")


def is_transient_error(exc: BaseException) -> bool:
    """True for blips worth one retry (503, rate limit, network) — not config/4xx."""
    if isinstance(exc, (StageTimeoutError, asyncio.TimeoutError, TimeoutError)):
        return False
    if isinstance(exc, (ConnectionError, OSError)):
        return True

    status = getattr(exc, "status_code", None) or getattr(exc, "status", None)
    if status is not None:
        try:
            code = int(status)
        except (TypeError, ValueError):
            code = None
        if code is not None:
            if code in (429, 500, 502, 503, 504):
                return True
            if 400 <= code < 500:
                return False

    message = str(exc).lower()
    if any(
        token in message
        for token in ("rate limit", "too many requests", "temporarily unavailable", "timeout")
    ):
        return True
    return False


def _remaining(deadline: float) -> float:
    return max(0.0, deadline - time.monotonic())


async def _jitter_sleep(min_ms: int, max_ms: int, *, deadline: float) -> None:
    delay = random.uniform(min_ms, max_ms) / 1000.0
    delay = min(delay, _remaining(deadline))
    if delay > 0:
        await asyncio.sleep(delay)


async def run_with_timeout_and_retry(
    factory: Callable[[], Awaitable[T]],
    *,
    stage: str,
    timeout_s: float,
    max_retries: int = 1,
    backoff_ms: tuple[int, int] = (200, 500),
    retryable: Callable[[BaseException], bool] = is_transient_error,
) -> T:
    """Run ``factory()`` under a stage deadline with at most one transient retry."""
    deadline = time.monotonic() + timeout_s
    attempts = 0
    while True:
        remaining = _remaining(deadline)
        if remaining <= 0:
            raise StageTimeoutError(stage, timeout_s)
        try:
            return await asyncio.wait_for(factory(), timeout=remaining)
        except asyncio.TimeoutError as exc:
            raise StageTimeoutError(stage, timeout_s) from exc
        except Exception as exc:
            if attempts < max_retries and retryable(exc) and _remaining(deadline) > 0:
                attempts += 1
                await _jitter_sleep(backoff_ms[0], backoff_ms[1], deadline=deadline)
                continue
            raise


async def stream_with_deadline(
    stream: AsyncIterator[T],
    *,
    stage: str,
    timeout_s: float,
    ttft_timeout_s: float | None = None,
) -> AsyncIterator[T]:
    """Yield items from ``stream`` until the stage deadline (optional TTFT cap)."""
    deadline = time.monotonic() + timeout_s
    ttft_deadline = time.monotonic() + ttft_timeout_s if ttft_timeout_s is not None else None
    got_first = False
    iterator = stream.__aiter__()

    while True:
        remaining = _remaining(deadline)
        if remaining <= 0:
            raise StageTimeoutError(stage, timeout_s)

        wait_for = remaining
        if ttft_deadline is not None and not got_first:
            ttft_remaining = ttft_deadline - time.monotonic()
            if ttft_remaining <= 0:
                raise StageTimeoutError(stage, ttft_timeout_s or timeout_s, ttft=True)
            wait_for = min(wait_for, ttft_remaining)

        try:
            item = await asyncio.wait_for(iterator.__anext__(), timeout=wait_for)
        except StopAsyncIteration:
            break
        except asyncio.TimeoutError as exc:
            if ttft_deadline is not None and not got_first:
                raise StageTimeoutError(stage, ttft_timeout_s or timeout_s, ttft=True) from exc
            raise StageTimeoutError(stage, timeout_s) from exc

        got_first = True
        yield item


async def stream_with_retry(
    factory: Callable[[], AsyncIterator[T]],
    *,
    stage: str,
    timeout_s: float,
    ttft_timeout_s: float | None = None,
    max_retries: int = 1,
    backoff_ms: tuple[int, int] = (200, 500),
    retryable: Callable[[BaseException], bool] = is_transient_error,
) -> AsyncIterator[T]:
    """Open a stream under deadline; retry once on transient open/first-item failure."""
    deadline = time.monotonic() + timeout_s
    attempts = 0
    while True:
        remaining = _remaining(deadline)
        if remaining <= 0:
            raise StageTimeoutError(stage, timeout_s)
        try:
            async for item in stream_with_deadline(
                factory(),
                stage=stage,
                timeout_s=remaining,
                ttft_timeout_s=ttft_timeout_s,
            ):
                yield item
            return
        except StageTimeoutError:
            raise
        except Exception as exc:
            if attempts < max_retries and retryable(exc) and _remaining(deadline) > 0:
                attempts += 1
                await _jitter_sleep(backoff_ms[0], backoff_ms[1], deadline=deadline)
                continue
            raise
