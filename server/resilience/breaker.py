"""Lightweight per-stage circuit breaker (issue #21).

Opens after ``failure_threshold`` consecutive hard failures (post-retry). Stays
open for ``cooldown_s``, then allows a single half-open probe.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

STAGE_NAMES = ("asr", "llm", "tts")


@dataclass
class CircuitBreaker:
    """Tracks consecutive failures for one pipeline stage."""

    failure_threshold: int
    cooldown_s: float
    consecutive_failures: int = 0
    opened_at: float | None = field(default=None, repr=False)
    half_open: bool = field(default=False, repr=False)

    def allow_request(self) -> bool:
        """Return whether a provider call may proceed."""
        if self.opened_at is None:
            return True
        if time.monotonic() - self.opened_at >= self.cooldown_s:
            self.half_open = True
            return True
        return False

    def is_blocking(self) -> bool:
        """True while requests are rejected (cooldown not yet elapsed)."""
        if self.opened_at is None:
            return False
        return time.monotonic() - self.opened_at < self.cooldown_s

    def cooldown_remaining_s(self) -> float:
        if self.opened_at is None:
            return 0.0
        return max(0.0, self.cooldown_s - (time.monotonic() - self.opened_at))

    def record_success(self) -> None:
        self.consecutive_failures = 0
        self.opened_at = None
        self.half_open = False

    def record_failure(self) -> bool:
        """Record a hard failure; return True if the breaker just opened."""
        if self.half_open:
            self.consecutive_failures = self.failure_threshold
            self.opened_at = time.monotonic()
            self.half_open = False
            return True

        self.consecutive_failures += 1
        if self.consecutive_failures >= self.failure_threshold:
            self.opened_at = time.monotonic()
            self.half_open = False
            return True
        return False


class StageBreakers:
    """One breaker per stage for a single WebSocket session."""

    def __init__(self, *, failure_threshold: int, cooldown_s: float) -> None:
        self._breakers = {
            stage: CircuitBreaker(failure_threshold, cooldown_s) for stage in STAGE_NAMES
        }

    def allow(self, stage: str) -> bool:
        return self._breakers[stage].allow_request()

    def is_open(self, stage: str) -> bool:
        return self._breakers[stage].is_blocking()

    def cooldown_remaining_s(self, stage: str) -> float:
        return self._breakers[stage].cooldown_remaining_s()

    def record_success(self, stage: str) -> None:
        self._breakers[stage].record_success()

    def record_failure(self, stage: str) -> bool:
        return self._breakers[stage].record_failure()
