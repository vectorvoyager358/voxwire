"""Issue #21 tests: per-stage circuit breaker."""

from __future__ import annotations

import asyncio
import base64

import pytest

from server.config import Settings
from server.pipeline.errors import BREAKER_OPEN, CONFIG_ERROR, MSG_ASR_BREAKER, PROVIDER_DOWN
from server.pipeline.orchestrator import PipelineOrchestrator
from server.providers.asr import ASRProvider, ASRSession, OnTranscript
from server.resilience.breaker import CircuitBreaker, StageBreakers
from tests.conftest import FakeASRProvider, FakeLLMProvider, FakeTTSProvider

_SILENCE = base64.b64encode(b"\x00\x00" * 160).decode()


class _Collector:
    def __init__(self) -> None:
        self.events: list[dict] = []

    async def send(self, payload: dict) -> None:
        self.events.append(payload)


class _FailASRProvider(ASRProvider):
    async def start(self, on_transcript: OnTranscript) -> ASRSession:
        raise RuntimeError("deepgram unavailable")


def test_circuit_breaker_opens_after_threshold() -> None:
    breaker = CircuitBreaker(failure_threshold=3, cooldown_s=60.0)
    assert breaker.allow_request() is True
    assert breaker.record_failure() is False
    assert breaker.record_failure() is False
    assert breaker.record_failure() is True
    assert breaker.is_blocking() is True
    assert breaker.allow_request() is False


def test_circuit_breaker_half_open_after_cooldown(monkeypatch: pytest.MonkeyPatch) -> None:
    now = 1000.0
    monkeypatch.setattr("server.resilience.breaker.time.monotonic", lambda: now)

    breaker = CircuitBreaker(failure_threshold=2, cooldown_s=30.0)
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.is_blocking() is True

    now += 30.0
    assert breaker.allow_request() is True
    assert breaker.half_open is True

    breaker.record_success()
    assert breaker.consecutive_failures == 0
    assert breaker.opened_at is None


def test_half_open_failure_reopens_breaker(monkeypatch: pytest.MonkeyPatch) -> None:
    now = 0.0
    monkeypatch.setattr("server.resilience.breaker.time.monotonic", lambda: now)

    breaker = CircuitBreaker(failure_threshold=2, cooldown_s=10.0)
    breaker.record_failure()
    breaker.record_failure()
    now += 10.0
    assert breaker.allow_request() is True

    assert breaker.record_failure() is True
    assert breaker.is_blocking() is True


def test_stage_breakers_are_independent() -> None:
    breakers = StageBreakers(failure_threshold=2, cooldown_s=60.0)
    breakers.record_failure("asr")
    breakers.record_failure("asr")
    assert breakers.is_open("asr") is True
    assert breakers.is_open("llm") is False
    assert breakers.allow("llm") is True


def test_single_turn_counts_one_asr_failure_despite_many_chunks(
    monkeypatch: pytest.MonkeyPatch,
    fake_llm: FakeLLMProvider,
    fake_tts: FakeTTSProvider,
) -> None:
    """One PTT utterance sends many chunks; ASR start must fail at most once per turn."""
    monkeypatch.setattr(
        "server.pipeline.orchestrator.get_asr_provider",
        lambda _s: _FailASRProvider(),
    )
    settings = Settings(breaker_failure_threshold=3, breaker_cooldown_s=60.0)

    async def run() -> list[dict]:
        collector = _Collector()
        orch = PipelineOrchestrator("sess-chunks", collector.send, settings=settings)
        turn_id = "t-chunks"
        for seq in range(10):
            await orch.on_audio_chunk({"turnId": turn_id, "seq": seq, "data": _SILENCE})
        await orch.on_utterance_end({"turnId": turn_id, "totalChunks": 10})
        return collector.events

    events = asyncio.run(run())
    asr_errors = [e for e in events if e["type"] == "error" and e["stage"] == "asr"]
    assert len(asr_errors) == 1
    assert asr_errors[0]["code"] == PROVIDER_DOWN
    assert not any(e.get("code") == BREAKER_OPEN for e in events)


def test_llm_config_errors_trip_breaker_in_one_session(
    monkeypatch: pytest.MonkeyPatch,
    fake_asr: FakeASRProvider,
    fake_tts: FakeTTSProvider,
) -> None:
    def _missing_key(_settings):
        raise RuntimeError("Missing required provider credentials:\n  gemini -> set GEMINI_API_KEY")

    monkeypatch.setattr("server.pipeline.orchestrator.get_llm_provider", _missing_key)
    settings = Settings(breaker_failure_threshold=3, breaker_cooldown_s=60.0)

    async def run() -> list[dict]:
        collector = _Collector()
        orch = PipelineOrchestrator("sess-llm-cfg", collector.send, settings=settings)
        for turn_id in ("t-1", "t-2", "t-3"):
            await orch.on_audio_chunk({"turnId": turn_id, "seq": 0, "data": _SILENCE})
            await orch.on_utterance_end({"turnId": turn_id, "totalChunks": 1})
        return collector.events

    events = asyncio.run(run())
    llm_config = [
        e
        for e in events
        if e["type"] == "error" and e["stage"] == "llm" and e["code"] == CONFIG_ERROR
    ]
    llm_breaker = [
        e
        for e in events
        if e["type"] == "error" and e["stage"] == "llm" and e["code"] == BREAKER_OPEN
    ]

    assert len(llm_config) == 2
    assert all(e["recoverable"] is True for e in llm_config)
    assert len(llm_breaker) == 1
    assert llm_breaker[0]["turnId"] == "t-3"
    assert llm_breaker[0]["recoverable"] is False


def test_asr_config_errors_trip_breaker_in_one_session(
    monkeypatch: pytest.MonkeyPatch,
    fake_llm: FakeLLMProvider,
    fake_tts: FakeTTSProvider,
) -> None:
    def _missing_key(_settings):
        raise RuntimeError(
            "Missing required provider credentials:\n  deepgram -> set DEEPGRAM_API_KEY"
        )

    monkeypatch.setattr("server.pipeline.orchestrator.get_asr_provider", _missing_key)
    settings = Settings(breaker_failure_threshold=3, breaker_cooldown_s=60.0)

    async def run() -> list[dict]:
        collector = _Collector()
        orch = PipelineOrchestrator("sess-cfg-brk", collector.send, settings=settings)
        for turn_id in ("t-1", "t-2", "t-3"):
            await orch.on_audio_chunk({"turnId": turn_id, "seq": 0, "data": _SILENCE})
            await orch.on_utterance_end({"turnId": turn_id, "totalChunks": 1})
        return collector.events

    events = asyncio.run(run())
    config_errors = [e for e in events if e["type"] == "error" and e["code"] == CONFIG_ERROR]
    breaker_errors = [e for e in events if e["type"] == "error" and e["code"] == BREAKER_OPEN]

    assert len(config_errors) == 2
    assert all(e["recoverable"] is True for e in config_errors)
    assert len(breaker_errors) == 1
    assert breaker_errors[0]["turnId"] == "t-3"


def test_orchestrator_asr_breaker_opens_after_repeated_failures(
    monkeypatch: pytest.MonkeyPatch,
    fake_llm: FakeLLMProvider,
    fake_tts: FakeTTSProvider,
) -> None:
    monkeypatch.setattr(
        "server.pipeline.orchestrator.get_asr_provider",
        lambda _s: _FailASRProvider(),
    )
    settings = Settings(breaker_failure_threshold=2, breaker_cooldown_s=60.0)

    async def run() -> list[dict]:
        collector = _Collector()
        orch = PipelineOrchestrator("sess-brk", collector.send, settings=settings)
        for turn_id in ("t-1", "t-2", "t-3"):
            await orch.on_audio_chunk({"turnId": turn_id, "seq": 0, "data": _SILENCE})
            await orch.on_utterance_end({"turnId": turn_id, "totalChunks": 1})
        return collector.events

    events = asyncio.run(run())
    provider_errors = [e for e in events if e["type"] == "error" and e["code"] == PROVIDER_DOWN]
    breaker_errors = [e for e in events if e["type"] == "error" and e["code"] == BREAKER_OPEN]

    assert len(provider_errors) == 1
    assert len(breaker_errors) == 2
    assert provider_errors[0]["turnId"] == "t-1"
    assert breaker_errors[0]["turnId"] == "t-2"
    assert breaker_errors[0]["stage"] == "asr"
    assert breaker_errors[0]["recoverable"] is False
    assert breaker_errors[0]["message"] == MSG_ASR_BREAKER
    assert breaker_errors[0]["cooldownMs"] > 0
    assert breaker_errors[1]["turnId"] == "t-3"

    complete = next(e for e in events if e["type"] == "turn_complete" and e["turnId"] == "t-3")
    assert complete["meta"]["degraded"] is True
    assert complete["meta"]["degradedMode"] == ["asr"]


def test_breaker_emits_on_opening_failure_not_next_request(
    monkeypatch: pytest.MonkeyPatch,
    fake_llm: FakeLLMProvider,
    fake_tts: FakeTTSProvider,
) -> None:
    """The Nth hard failure should emit BREAKER_OPEN immediately (not on attempt N+1)."""
    monkeypatch.setattr(
        "server.pipeline.orchestrator.get_asr_provider",
        lambda _s: _FailASRProvider(),
    )
    settings = Settings(breaker_failure_threshold=3, breaker_cooldown_s=60.0)

    async def run() -> list[dict]:
        collector = _Collector()
        orch = PipelineOrchestrator("sess-open-now", collector.send, settings=settings)
        for turn_id in ("t-1", "t-2", "t-3"):
            await orch.on_audio_chunk({"turnId": turn_id, "seq": 0, "data": _SILENCE})
            await orch.on_utterance_end({"turnId": turn_id, "totalChunks": 1})
        return collector.events

    events = asyncio.run(run())
    third_turn_errors = [e for e in events if e["type"] == "error" and e["turnId"] == "t-3"]
    assert len(third_turn_errors) == 1
    assert third_turn_errors[0]["code"] == BREAKER_OPEN
    assert third_turn_errors[0]["recoverable"] is False


def test_success_resets_breaker(
    monkeypatch: pytest.MonkeyPatch,
    fake_llm: FakeLLMProvider,
    fake_tts: FakeTTSProvider,
) -> None:
    from tests.conftest import FakeASRProvider

    fail = _FailASRProvider()
    ok = FakeASRProvider()
    use_fail = True

    def _provider(_settings):
        return fail if use_fail else ok

    monkeypatch.setattr("server.pipeline.orchestrator.get_asr_provider", _provider)
    settings = Settings(breaker_failure_threshold=2, breaker_cooldown_s=60.0)

    async def run() -> list[dict]:
        nonlocal use_fail
        collector = _Collector()
        orch = PipelineOrchestrator("sess-reset", collector.send, settings=settings)
        use_fail = True
        await orch.on_audio_chunk({"turnId": "t-fail", "seq": 0, "data": _SILENCE})
        await orch.on_utterance_end({"turnId": "t-fail", "totalChunks": 1})
        use_fail = False
        await orch.on_audio_chunk({"turnId": "t-ok", "seq": 0, "data": _SILENCE})
        await orch.on_utterance_end({"turnId": "t-ok", "totalChunks": 1})
        return collector.events

    events = asyncio.run(run())
    assert not any(e.get("code") == BREAKER_OPEN for e in events)
    assert fake_llm.received == ["hello world"]
