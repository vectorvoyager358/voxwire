"""Per-turn latency marks and stage breakdown (issue #15).

See ``docs/latency-budget.md`` for mark names, formulas, and wire format.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

# Marks that always update (most recent wins).
_ALWAYS_UPDATE = frozenset({"last_audio_chunk"})

# Pipeline stages eligible for timeline bottleneck (excludes tail "Finish" segment).
_TIMELINE_MILESTONES: tuple[tuple[str, str, str], ...] = (
    ("asrFinalMs", "asr", "ASR"),
    ("llmTtftMs", "llm", "LLM"),
    ("ttsTtfbMs", "tts", "TTS"),
)


def _fallback_bottleneck_stage(stages: dict[str, int | None]) -> str | None:
    """Last-resort label when the turn is too fast for timeline segments (e.g. fakes)."""
    for stage, key in (("tts", "ttsCompleteMs"), ("llm", "llmCompleteMs"), ("asr", "asrFinalMs")):
        if stages.get(key) is not None:
            return stage
    return None


def _timeline_segments(stages: dict[str, int | None], total_ms: int) -> list[tuple[str, int]]:
    """Non-overlapping server segments ``(stage, duration_ms)`` that sum to ``total_ms``."""
    if total_ms <= 0:
        return []

    milestones: list[tuple[int, str]] = []
    for key, stage, _label in _TIMELINE_MILESTONES:
        ms = stages.get(key)
        if ms is not None and ms > 0:
            milestones.append((min(ms, total_ms), stage))

    milestones.sort(key=lambda pair: pair[0])
    unique: list[tuple[int, str]] = []
    for ms, stage in milestones:
        if unique and unique[-1][0] == ms:
            unique[-1] = (ms, stage)
        elif not unique or ms > unique[-1][0]:
            unique.append((ms, stage))

    ticks = [0, *[ms for ms, _ in unique]]
    if ticks[-1] < total_ms:
        ticks.append(total_ms)

    segments: list[tuple[str, int]] = []
    for i in range(len(ticks) - 1):
        duration = ticks[i + 1] - ticks[i]
        if duration <= 0:
            continue
        stage = next((s for ms, s in unique if ms == ticks[i + 1]), "overhead")
        segments.append((stage, duration))
    return segments


def _union_length(intervals: list[tuple[float, float]]) -> float:
    """Total length of the union of half-open intervals ``[start, end)``."""
    if not intervals:
        return 0.0
    ordered = sorted(intervals, key=lambda pair: pair[0])
    merged_start, merged_end = ordered[0]
    total = 0.0
    for start, end in ordered[1:]:
        if start <= merged_end:
            merged_end = max(merged_end, end)
        else:
            total += merged_end - merged_start
            merged_start, merged_end = start, end
    total += merged_end - merged_start
    return total


@dataclass
class LatencyTracker:
    """Accumulates ``time.perf_counter()`` marks for one turn; builds stage report."""

    _marks: dict[str, float] = field(default_factory=dict)
    _t0: float | None = field(default=None, repr=False)
    _client_capture_ms: int | None = field(default=None, repr=False)
    _tts_skipped: bool = field(default=False, repr=False)
    _failed_stage: str | None = field(default=None, repr=False)

    @property
    def anchor_set(self) -> bool:
        """True after ``utterance_end`` (T₀) is marked."""
        return self._t0 is not None

    def begin_turn(self) -> None:
        """Reset marks for a new ``turnId``."""
        self._marks.clear()
        self._t0 = None
        self._client_capture_ms = None
        self._tts_skipped = False
        self._failed_stage = None

    def set_failed_stage(self, stage: str) -> None:
        """Record the first failed pipeline stage for this turn (``asr``/``llm``/``tts``)."""
        if self._failed_stage is None:
            self._failed_stage = stage

    def mark(self, name: str, *, at: float | None = None) -> None:
        """Record a named mark. First occurrence wins except ``last_audio_chunk``."""
        t = at if at is not None else time.perf_counter()
        if name in _ALWAYS_UPDATE or name not in self._marks:
            self._marks[name] = t
        if name == "utterance_end":
            self._t0 = t

    def set_client_capture_ms(self, ms: int | None) -> None:
        self._client_capture_ms = ms

    def set_tts_skipped(self, skipped: bool) -> None:
        self._tts_skipped = skipped

    def _ms_since_t0(self, name: str) -> int | None:
        if self._t0 is None or name not in self._marks:
            return None
        return round((self._marks[name] - self._t0) * 1000)

    def ms_since_utterance_end(self, mark: str) -> int | None:
        """Milliseconds from T₀ (``utterance_end``) to a named mark, if recorded."""
        return self._ms_since_t0(mark)

    def _ms_between(self, start: str, end: str) -> int | None:
        if start not in self._marks or end not in self._marks:
            return None
        delta = (self._marks[end] - self._marks[start]) * 1000
        return round(max(0.0, delta))

    def _overhead_ms(self, total_ms: int) -> int:
        if self._t0 is None:
            return 0
        intervals: list[tuple[float, float]] = []
        if "asr_final" in self._marks:
            intervals.append((self._t0, self._marks["asr_final"]))
        if "llm_start" in self._marks and "llm_complete" in self._marks:
            intervals.append((self._marks["llm_start"], self._marks["llm_complete"]))
        if not self._tts_skipped and "tts_start" in self._marks and "tts_complete" in self._marks:
            intervals.append((self._marks["tts_start"], self._marks["tts_complete"]))
        busy_ms = round(_union_length(intervals) * 1000)
        return max(0, total_ms - busy_ms)

    def _build_stages(self, total: int) -> dict[str, int | None]:
        return {
            "clientCaptureMs": self._client_capture_ms,
            "audioUploadMs": self._ms_between("last_audio_chunk", "utterance_end"),
            "asrFirstPartialMs": self._ms_since_t0("asr_first_partial"),
            "asrFinalMs": self._ms_since_t0("asr_final"),
            "llmTtftMs": self._ms_since_t0("llm_first_token"),
            "llmCompleteMs": self._ms_since_t0("llm_complete"),
            "ttsTtfbMs": None if self._tts_skipped else self._ms_since_t0("tts_first_byte"),
            "ttsCompleteMs": None if self._tts_skipped else self._ms_since_t0("tts_complete"),
            "orchestrationOverheadMs": self._overhead_ms(total),
        }

    @staticmethod
    def _bottleneck_stage(stages: dict[str, int | None], total_ms: int) -> str | None:
        """Slowest pipeline segment on the server timeline (matches client bar widths)."""
        segments = _timeline_segments(stages, total_ms)
        pipeline = [(stage, dur) for stage, dur in segments if stage != "overhead"]
        pool = pipeline if pipeline else segments
        best_label: str | None = None
        best_ms = -1
        for stage, duration in pool:
            if duration > best_ms:
                best_ms = duration
                best_label = stage
        return best_label if best_label is not None else _fallback_bottleneck_stage(stages)

    def build_report(self, *, degraded: bool = False) -> dict:
        """Return latency breakdown with bottleneck, failure info, and summary meta."""
        total = self._ms_since_t0("turn_complete")
        if total is None:
            total = 0
            stages = self._empty_stages()
        else:
            stages = self._build_stages(total)

        bottleneck = self._bottleneck_stage(stages, total)
        summary_meta = {
            "totalMs": total,
            "bottleneckStage": bottleneck,
            "failedStage": self._failed_stage,
            "degraded": degraded,
        }
        return {
            "totalMs": total,
            "stages": stages,
            "bottleneckStage": bottleneck,
            "failedStage": self._failed_stage,
            "meta": summary_meta,
        }

    @staticmethod
    def _empty_stages() -> dict[str, int | None]:
        return {
            "clientCaptureMs": None,
            "audioUploadMs": None,
            "asrFirstPartialMs": None,
            "asrFinalMs": None,
            "llmTtftMs": None,
            "llmCompleteMs": None,
            "ttsTtfbMs": None,
            "ttsCompleteMs": None,
            "orchestrationOverheadMs": None,
        }
