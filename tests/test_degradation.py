"""Issue #20 tests: graceful degradation and structured errors."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import AsyncIterator

import pytest

from server.config import Settings
from server.pipeline.errors import CONFIG_ERROR, MSG_ASR_DOWN, MSG_TTS_DOWN, TIMEOUT
from server.pipeline.orchestrator import LLM_TIMEOUT_FALLBACK, PipelineOrchestrator
from server.providers.tts import TTSProvider
from tests.conftest import FakeASRProvider, FakeLLMProvider, FakeTTSProvider

_SILENCE = base64.b64encode(b"\x00\x00" * 160).decode()


class _Collector:
    def __init__(self) -> None:
        self.events: list[dict] = []

    async def send(self, payload: dict) -> None:
        self.events.append(payload)


class _FailingTTSProvider(TTSProvider):
    async def stream(self, text: str) -> AsyncIterator[bytes]:
        raise RuntimeError("cartesia unavailable")
        yield b""  # pragma: no cover


def test_llm_config_error_allows_in_session_retry(
    monkeypatch: pytest.MonkeyPatch,
    fake_asr: FakeASRProvider,
) -> None:
    def _raise(_settings):
        raise RuntimeError("Missing required provider credentials:\n  gemini -> set GEMINI_API_KEY")

    monkeypatch.setattr("server.pipeline.orchestrator.get_llm_provider", _raise)

    async def run() -> list[dict]:
        collector = _Collector()
        orch = PipelineOrchestrator("sess-cfg", collector.send)
        await orch.on_audio_chunk({"turnId": "t-cfg", "seq": 0, "data": _SILENCE})
        await orch.on_utterance_end({"turnId": "t-cfg", "totalChunks": 1})
        return collector.events

    events = asyncio.run(run())
    err = next(e for e in events if e["type"] == "error")
    assert err["code"] == CONFIG_ERROR
    assert err["recoverable"] is True
    assert events[-1]["meta"]["degraded"] is True
    assert events[-1]["meta"]["ttsSkipped"] is True


def test_tts_failure_sets_tts_skipped(
    monkeypatch: pytest.MonkeyPatch,
    fake_asr: FakeASRProvider,
    fake_llm: FakeLLMProvider,
) -> None:
    monkeypatch.setattr(
        "server.pipeline.orchestrator.get_tts_provider",
        lambda _s: _FailingTTSProvider(),
    )

    async def run() -> list[dict]:
        collector = _Collector()
        orch = PipelineOrchestrator("sess-tts", collector.send)
        await orch.on_audio_chunk({"turnId": "t-tts", "seq": 0, "data": _SILENCE})
        await orch.on_utterance_end({"turnId": "t-tts", "totalChunks": 1})
        return collector.events

    events = asyncio.run(run())
    err = next(e for e in events if e["type"] == "error" and e["stage"] == "tts")
    assert err["message"] == MSG_TTS_DOWN
    assert err["recoverable"] is True
    complete = events[-1]
    assert complete["meta"]["ttsSkipped"] is True
    assert any(e["type"] == "llm_complete" for e in events)


def test_text_turn_skips_asr(
    fake_llm: FakeLLMProvider,
    fake_tts: FakeTTSProvider,
) -> None:
    async def run() -> list[dict]:
        collector = _Collector()
        orch = PipelineOrchestrator("sess-text", collector.send)
        await orch.on_text_turn({"turnId": "t-text", "text": "hello there"})
        return collector.events

    events = asyncio.run(run())
    assert not any(e["type"] == "transcript_partial" for e in events)
    final = next(e for e in events if e["type"] == "transcript_final")
    assert final["text"] == "hello there"
    assert fake_llm.received == ["hello there"]
    assert events[-1]["type"] == "turn_complete"


def test_asr_timeout_user_message(
    monkeypatch: pytest.MonkeyPatch,
    fake_llm: FakeLLMProvider,
    fake_tts: FakeTTSProvider,
) -> None:
    from tests.test_timeouts import _SlowASRProvider

    monkeypatch.setattr(
        "server.pipeline.orchestrator.get_asr_provider",
        lambda _s: _SlowASRProvider(),
    )
    settings = Settings(asr_timeout_s=0.01)

    async def run() -> list[dict]:
        collector = _Collector()
        orch = PipelineOrchestrator("sess-asr-msg", collector.send, settings=settings)
        await orch.on_audio_chunk({"turnId": "t-asr-msg", "seq": 0, "data": _SILENCE})
        await orch.on_utterance_end({"turnId": "t-asr-msg", "totalChunks": 1})
        return collector.events

    events = asyncio.run(run())
    err = next(e for e in events if e["type"] == "error")
    assert err["code"] == TIMEOUT
    assert err["message"] == MSG_ASR_DOWN
    assert err["recoverable"] is True
    assert fake_llm.received == []


def test_llm_ttft_timeout_skips_tts(
    monkeypatch: pytest.MonkeyPatch,
    fake_asr: FakeASRProvider,
    fake_tts: FakeLLMProvider,
) -> None:
    from tests.test_timeouts import _SlowLLMProvider

    monkeypatch.setattr(
        "server.pipeline.orchestrator.get_llm_provider",
        lambda _s: _SlowLLMProvider(),
    )
    settings = Settings(llm_timeout_s=1.0, llm_ttft_timeout_s=0.01)

    async def run() -> list[dict]:
        collector = _Collector()
        orch = PipelineOrchestrator("sess-llm-skip", collector.send, settings=settings)
        await orch.on_audio_chunk({"turnId": "t-llm-skip", "seq": 0, "data": _SILENCE})
        await orch.on_utterance_end({"turnId": "t-llm-skip", "totalChunks": 1})
        return collector.events

    events = asyncio.run(run())
    llm_complete = next(e for e in events if e["type"] == "llm_complete")
    assert llm_complete["text"] == LLM_TIMEOUT_FALLBACK
    assert not any(e["type"] == "tts_audio_chunk" for e in events)
    assert events[-1]["meta"]["ttsSkipped"] is True
