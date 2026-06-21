"""Issue #19 tests: orchestrator timeout handling."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import AsyncIterator

import pytest

from server.config import Settings
from server.pipeline.orchestrator import LLM_TIMEOUT_FALLBACK, PipelineOrchestrator
from server.providers.asr import ASRProvider, ASRSession, OnTranscript
from server.providers.llm import LLMProvider
from server.providers.tts import TTSProvider
from tests.conftest import FakeASRProvider, FakeLLMProvider, FakeTTSProvider

_SILENCE = base64.b64encode(b"\x00\x00" * 160).decode()


class _Collector:
    def __init__(self) -> None:
        self.events: list[dict] = []

    async def send(self, payload: dict) -> None:
        self.events.append(payload)


class _SlowFinalizeSession(ASRSession):
    def __init__(self, on_transcript: OnTranscript) -> None:
        self.closed = False

    async def send_audio(self, pcm: bytes) -> None:
        return None

    async def finalize(self) -> str:
        await asyncio.sleep(0.05)
        return "too late"

    async def aclose(self) -> None:
        self.closed = True


class _SlowASRProvider(ASRProvider):
    async def start(self, on_transcript: OnTranscript) -> ASRSession:
        return _SlowFinalizeSession(on_transcript)


class _SlowLLMProvider(LLMProvider):
    async def stream(self, user_text: str) -> AsyncIterator[str]:
        await asyncio.sleep(0.05)
        yield "late"


class _SlowTTSProvider(TTSProvider):
    async def stream(self, text: str) -> AsyncIterator[bytes]:
        await asyncio.sleep(0.05)
        yield b"\x01\x02"


def test_asr_timeout_emits_error_without_llm(
    monkeypatch: pytest.MonkeyPatch,
    fake_llm: FakeLLMProvider,
    fake_tts: FakeTTSProvider,
) -> None:
    monkeypatch.setattr(
        "server.pipeline.orchestrator.get_asr_provider",
        lambda _s: _SlowASRProvider(),
    )
    settings = Settings(asr_timeout_s=0.01)

    async def run() -> list[dict]:
        collector = _Collector()
        orch = PipelineOrchestrator("sess-asr-to", collector.send, settings=settings)
        await orch.on_audio_chunk({"turnId": "t-asr", "seq": 0, "data": _SILENCE})
        await orch.on_utterance_end({"turnId": "t-asr", "totalChunks": 1})
        return collector.events

    events = asyncio.run(run())
    errors = [e for e in events if e["type"] == "error"]
    assert errors[0]["stage"] == "asr"
    assert errors[0]["code"] == "TIMEOUT"
    assert fake_llm.received == []
    assert events[-1]["meta"]["degraded"] is True


def test_llm_ttft_timeout_uses_canned_reply(
    monkeypatch: pytest.MonkeyPatch,
    fake_asr: FakeASRProvider,
    fake_tts: FakeTTSProvider,
) -> None:
    monkeypatch.setattr(
        "server.pipeline.orchestrator.get_llm_provider",
        lambda _s: _SlowLLMProvider(),
    )
    settings = Settings(llm_timeout_s=1.0, llm_ttft_timeout_s=0.01)

    async def run() -> list[dict]:
        collector = _Collector()
        orch = PipelineOrchestrator("sess-llm-to", collector.send, settings=settings)
        await orch.on_audio_chunk({"turnId": "t-llm", "seq": 0, "data": _SILENCE})
        await orch.on_utterance_end({"turnId": "t-llm", "totalChunks": 1})
        return collector.events

    events = asyncio.run(run())
    llm_complete = next(e for e in events if e["type"] == "llm_complete")
    assert llm_complete["text"] == LLM_TIMEOUT_FALLBACK
    errors = [e for e in events if e["type"] == "error" and e["stage"] == "llm"]
    assert errors and errors[0]["code"] == "TIMEOUT"


def test_tts_timeout_keeps_text_only(
    monkeypatch: pytest.MonkeyPatch,
    fake_asr: FakeASRProvider,
    fake_llm: FakeLLMProvider,
) -> None:
    monkeypatch.setattr(
        "server.pipeline.orchestrator.get_tts_provider",
        lambda _s: _SlowTTSProvider(),
    )
    settings = Settings(tts_timeout_s=0.01)

    async def run() -> list[dict]:
        collector = _Collector()
        orch = PipelineOrchestrator("sess-tts-to", collector.send, settings=settings)
        await orch.on_audio_chunk({"turnId": "t-tts", "seq": 0, "data": _SILENCE})
        await orch.on_utterance_end({"turnId": "t-tts", "totalChunks": 1})
        return collector.events

    events = asyncio.run(run())
    assert not any(e["type"] == "tts_audio_chunk" for e in events)
    errors = [e for e in events if e["type"] == "error" and e["stage"] == "tts"]
    assert errors and errors[0]["code"] == "TIMEOUT"
    llm_complete = next(e for e in events if e["type"] == "llm_complete")
    assert llm_complete["text"] == "Hi there!"
